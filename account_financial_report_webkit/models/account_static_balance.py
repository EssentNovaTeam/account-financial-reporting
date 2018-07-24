# coding: utf-8
# Copyright (C) 2018 DynApps <http://www.dynapps.be>
# @author Pieter Paulussen <pieter.paulussen@dynapps.be>
# @author Stefan Rijnhart <stefan.rijnhart@dynapps.nl>
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).
import logging
import psycopg2
from time import time
from openerp import api, fields, models

logger = logging.getLogger(__name__)


class AccountStaticBalance(models.Model):
    _name = 'account.static.balance'

    account_id = fields.Many2one(
        comodel_name='account.account', required=True, index=True)
    period_id = fields.Many2one(
        comodel_name='account.period', required=True, index=True)
    credit = fields.Float(default=0.0)
    debit = fields.Float(default=0.0)
    balance = fields.Float(default=0.0)
    curr_balance = fields.Float(default=0.0)

    _sql_constraints = [
        ('account_static_balance_unique',
         'unique (account_id, period_id)',
         'Only 1 unique combination of account and period is allowed.'),
    ]

    @api.multi
    def get_entries_to_calculate(self, periods=None):
        """ Calculates all possible combinations of accounts and closed
        periods which are missing from the expected result.
        """
        query = """
          WITH balances AS (
            SELECT ap.id as period, aa.id as account, asb.id as balance
            FROM account_period ap
            CROSS JOIN account_account aa
            LEFT JOIN account_static_balance asb
              ON asb.account_id = aa.id AND asb.period_id = ap.id
            WHERE ap.state = 'done' and not ap.special {})
          SELECT period, account FROM balances WHERE balance IS NULL;
        """.format('AND ap.id IN %(period_ids)s' if periods else '')
        self.env.cr.execute(query, {'period_ids': tuple(periods.ids)})
        return self.env.cr.fetchall()

    @api.multi
    def unlink(self):
        """ When manually unlinking, trigger a recalculation of the entry. """
        accounts = self.mapped('account_id')
        periods = self.mapped('period_id')
        res = super(AccountStaticBalance, self).unlink()
        if not self.env.context.get('skip_recalculation', False):
            # Trigger recalculation of the entry
            self.env['account.static.balance'].calculate_static_balance(
                periods=periods, accounts=accounts)
        return res

    @api.model
    def auto_populate_table(self):
        """ ir.cron function to trigger intial balance calculation for all
        closed periods and accounts if their combination is missing. """
        periods, accounts = self.get_missing_periods_and_accounts()
        if not periods:
            logger.debug("No missing entries found. Static account balance "
                         "data is up to date.")
            return
        self.calculate_static_balance(periods=periods, accounts=accounts)

    @api.multi
    def get_missing_periods_and_accounts(self, periods=None):
        # Split into unique periods and accounts
        per_ids = acc_ids = []
        missing_combinations = self.get_entries_to_calculate(periods=periods)
        if missing_combinations:
            per_ids, acc_ids = map(list, map(set, zip(*missing_combinations)))
        missing_periods = self.env['account.period'].browse(per_ids)
        missing_accounts = self.env['account.account'].browse(acc_ids)
        return missing_periods, missing_accounts

    @api.model
    def calculate_static_balance(self, periods, accounts=None):
        """ Calculate the balance for a specified set of periods
        and insert them into the table."""
        self = self.suspend_security()
        periods = periods.filtered(
            lambda p: p.state == 'done' and not p.special)
        for row in self.calculate_balance(periods, accounts=accounts):
            try:
                with self.env.cr.savepoint():
                    self.create(row)
            except psycopg2.IntegrityError:
                # A balance already exists for this period and account
                pass

    @api.model
    def calculate_balance(self, periods, accounts=None, include_draft=False):
        """Calculate the balance for a specified set of periods
        :param periods: periods to calculate the values for
        :param accounts: optional accounts to calculate the values for
        :return: True
        """
        if not periods:
            return {}
        if accounts is None:
            # Calculate for all accounts
            accounts = self.env['account.account'].search([])

        logger.debug(
            "Starting calculation of balances for "
            "periods: %s.", ", ".join(periods.mapped('name')))

        start_time = time()

        # Calculate the values for the periods
        query = """
            WITH expanded AS (
              SELECT
                ap.id AS period_id,
                aa.id AS account_id,
                0 AS debit,
                0 AS credit,
                0 AS balance,
                0 AS curr_balance
              FROM account_account aa
              CROSS JOIN account_period ap
              WHERE ap.id IN %(period_ids)s AND aa.id in %(account_ids)s
              UNION ALL
              SELECT
                ap.id AS period_id,
                aa.id AS account_id,
                COALESCE(SUM(debit), 0) AS debit,
                COALESCE(SUM(credit), 0) AS credit,
                COALESCE(sum(debit), 0) - COALESCE(sum(credit), 0) AS balance,
                COALESCE(sum(amount_currency), 0) AS curr_balance
              FROM account_move_line aml
              JOIN account_account aa ON aa.id = account_id
              JOIN account_period ap ON ap.id = aml.period_id
              JOIN account_move am ON aml.move_id = am.id
              WHERE ap.id IN %(period_ids)s AND aa.id in %(account_ids)s
                {}  -- AND am.state == 'posted'
              GROUP BY ap.id, aa.id)
            SELECT
              period_id AS period_id,
              account_id AS account_id,
              SUM(debit) AS debit,
              SUM(credit) AS credit,
              sum(balance) as balance,
              sum(curr_balance) as curr_balance
              FROM expanded
            GROUP BY period_id, account_id
        """.format('AND am.state = \'posted\'' if not include_draft else '')
        self.env.cr.execute(query, {
            'period_ids': tuple(periods.ids),
            'account_ids': tuple(accounts.ids),
        })
        time_taken = time() - start_time
        hours, rest = divmod(time_taken, 3600)
        minutes, seconds = divmod(rest, 60)
        logger.debug(
            "Completed calculation of balances for periods %s and %s "
            "accounts. Total duration: %.0f hours %.0f minutes %.0f seconds.",
            ", ".join(periods.mapped('name')),
            len(accounts), hours, minutes, seconds)
        return self.env.cr.dictfetchall()

    @api.model
    def get_balances(self, accounts, periods, include_draft=False,
                     consolidate=True):
        """
        Calculate the credit, debit and balance of each account in a specified
        set of periods. Split up the calculation logic to static and dynamic
        computations. The balances include the amounts from child and
        consolidated accounts (but these accounts are not added to the list of
        returned accounts if not already present).
        :param accounts: dict of specific account values
        :param period_ids: the ids of the periods
        :param include_draft: include unposted moves
        :param consolidate: include child and consolidated accounts
        :return: dictionary of account ids to values
        """
        if not accounts or not periods:
            return {}
        return_ids = accounts.ids  # Don't return child and consolitated
        if consolidate:
            accounts |= self.env['account.account'].browse(
                accounts._get_children_and_consol())

        fields_to_read = [
            'id', 'type', 'code', 'name', 'parent_id', 'level', 'child_id']
        res = dict((account['id'], account)
                   for account in accounts.read(fields_to_read))

        def map_data_to_account_id(data):
            for values in data:
                entry = res[values['account_id']]
                entry.update({
                    'credit': entry.get('credit', 0) + values['credit'],
                    'debit': entry.get('debit', 0) + values['debit'],
                    'balance': entry.get('balance', 0) + values['balance'],
                    'curr_balance': entry.get(
                        'curr_balance', 0) + values['curr_balance'],
                })

        static_periods = periods.filtered(
            lambda p: p.state == 'done' and not p.special)
        if static_periods:
            static_periods -= self.get_missing_periods_and_accounts(
                periods=static_periods)[0]
        compute_periods = periods - static_periods

        if compute_periods:
            map_data_to_account_id(
                self.calculate_balance(
                    compute_periods, accounts=accounts,
                    include_draft=include_draft))

        # Aggregate the static balance values to the current ones
        if static_periods:
            self.env.cr.execute("""
                SELECT account_id,
                  SUM(credit) AS credit,
                  SUM(debit) AS debit,
                  SUM(balance) as balance,
                  SUM(curr_balance) AS curr_balance
                FROM account_static_balance
                WHERE account_id IN %s
                AND period_id IN %s
                GROUP BY account_id;
                """, (tuple(accounts.ids), tuple(static_periods.ids)))
            map_data_to_account_id(self.env.cr.dictfetchall())

        if consolidate:
            values = res.copy()
            for account in res.values():
                # Get all children accounts
                for child_id in self.env['account.account'].browse(
                        account['id'])._get_children_and_consol():
                    if child_id == account['id']:
                        continue
                    entry = values[child_id]
                    # We have static data for the account so we sum
                    # the values
                    account.update({
                        'credit': account['credit'] + entry['credit'],
                        'debit': account['debit'] + entry['debit'],
                        'balance': account['balance'] + entry['balance'],
                        'curr_balance': (account['curr_balance'] +
                                         entry['curr_balance']),
                    })
        return dict(item for item in res.iteritems() if item[0] in return_ids)
