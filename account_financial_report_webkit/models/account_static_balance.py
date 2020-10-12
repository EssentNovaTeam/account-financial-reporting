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
    journal_id = fields.Many2one(
        'account.journal', required=True, index=True)
    credit = fields.Float(default=0.0)
    debit = fields.Float(default=0.0)
    balance = fields.Float(default=0.0)
    curr_balance = fields.Float(default=0.0)

    _sql_constraints = [
        ('account_static_balance_unique',
         'unique (account_id, period_id, journal_id)',
         'Only 1 unique combination of account, journal and period is '
         'allowed.'),
    ]

    @api.multi
    def get_entries_to_calculate(self, periods=None):
        """ Calculates all possible combinations of accounts and closed
        periods which are missing from the expected result.
        """
        query = """
        SELECT ap.id, aa.id, aj.id
            FROM account_account aa, account_journal aj, account_period ap
            WHERE NOT EXISTS (
                SELECT * FROM account_static_balance asb
                WHERE asb.account_id = aa.id
                    AND asb.journal_id = aj.id
                    AND asb.period_id = ap.id
                    AND EXISTS (
                        SELECT * FROM account_journal_period ajp
                          WHERE ajp.period_id = asb.period_id
                          AND ajp.journal_id = asb.journal_id
                          AND asb.create_date > ajp.write_date
                          OR ajp.state != 'done'))
                AND EXISTS (
                SELECT 1 FROM account_move_line aml
                WHERE aml.account_id = aa.id
                    AND aml.journal_id = aj.id
                    AND aml.period_id = ap.id)
                AND NOT ap.special AND ap.state='done' {};
        """.format('AND ap.id IN %(period_ids)s' if periods else '')
        self.env.cr.execute(query, {
            'period_ids': tuple(periods.ids) if periods else None})
        return self.env.cr.fetchall()

    @api.multi
    def unlink(self):
        """ When manually unlinking, trigger a recalculation of the entry. """
        accounts = self.mapped('account_id')
        periods = self.mapped('period_id')
        journals = self.mapped('journal_id')
        res = super(AccountStaticBalance, self).unlink()
        if not self.env.context.get('skip_recalculation', False):
            # Trigger recalculation of the entry
            self.env['account.static.balance'].calculate_static_balance(
                periods=periods, accounts=accounts, journals=journals)
        return res

    @api.model
    def auto_populate_table(self):
        """ ir.cron function to trigger intial balance calculation for all
        closed periods and accounts if their combination is missing. """
        periods, accounts, journals = \
            self.get_missing_periods_accounts_and_journals()
        if not periods:
            logger.debug("No missing entries found. Static account balance "
                         "data is up to date.")
            return
        self.calculate_static_balance(
            periods=periods, accounts=accounts, journals=journals)

    @api.multi
    def get_missing_periods_accounts_and_journals(self, periods=None):
        # Split into unique periods and accounts
        per_ids = []
        acc_ids = []
        journal_ids = []
        missing_combinations = self.get_entries_to_calculate(periods=periods)
        if missing_combinations:
            per_ids, acc_ids, journal_ids = map(
                list, map(set, zip(*missing_combinations)))
        missing_periods = self.env['account.period'].browse(per_ids)
        missing_accounts = self.env['account.account'].browse(acc_ids)
        missing_journals = self.env['account.journal'].browse(journal_ids)
        return missing_periods, missing_accounts, missing_journals

    @api.model
    def calculate_static_balance(self, periods, accounts=None, journals=None):
        """ Calculate the balance for a specified set of periods
        and insert them into the table."""
        self = self.suspend_security()

        if not journals:
            # When journals are set, we can assume that this is a close for
            # a journal period. Otherwise we need to filter the periods to
            # check if they are in a done state
            periods = periods.filtered(
                lambda p: p.state == 'done' and not p.special)

        for period in periods:
            for row in self.calculate_balance(
                    period, accounts=accounts, journals=journals):
                try:
                    with self.env.cr.savepoint():
                        logging.info(
                            'Calculated static balance for period %s in '
                            'journal %s and account %s',
                            row.get('period_id'), row.get('journal_id'),
                            row.get('account_id'))

                        # Unlink if there is already a static balance record
                        self.env['account.static.balance'].search([
                            ('journal_id', '=', row.get('journal_id')),
                            ('account_id', '=', row.get('account_id')),
                            ('period_id', '=', row.get('period_id')),
                        ]).with_context(skip_recalculation=True).unlink()

                        self.create(row)
                except psycopg2.IntegrityError as e:
                    # A balance already exists for this period and account
                    logger.exception(e)
                    pass

    @api.model
    def calculate_balance(self, period, accounts=None, include_draft=False,
                          journals=None):
        """
        Calculate the balance for a specified set of periods
        :param period: period to calculate the values for
        :param accounts: optional accounts to calculate the values for
        :param journals: optional journals to calculate the values for
        :return: True
        """
        if not period:
            return {}

        logger.debug(
            "Starting calculation of balances for "
            "periods: %s.", period.name)

        start_time = time()

        # Remove static balances with integrity errors
        self._validate_integrity()

        existing_journals = self.env['account.static.balance'].search([
            ('period_id', '=', period.id)
        ]).mapped('journal_id')

        query = """
        SELECT aj.id FROM account_journal aj
            WHERE EXISTS(
                SELECT FROM account_move_line aml
                WHERE period_id = %s
                    AND journal_id = aj.id)
        """
        self.env.cr.execute(query, (period.id,))
        all_journals = self.env['account.journal'].search([
            ('id', 'in', self.env.cr.fetchall())
        ])

        if existing_journals == all_journals:
            logger.info(
                'Skipped calculation of balance because all journals are '
                'already calculated for period %s', period.name)
            return {}

        if not journals:
            journals = all_journals - existing_journals
        else:
            # Dont recalculate journals that are already correctly calculated
            journals = journals - existing_journals

        if not journals:
            logger.info(
                'Skipped calculation of balance because all journals are '
                'already calculated for period %s', period.name)
            return {}

        # Calculate the values for the periods
        query = """
            SELECT
                aml.period_id AS period_id,
                aml.account_id AS account_id,
                aml.journal_id AS journal_id,
                COALESCE(SUM(debit), 0) AS debit,
                COALESCE(SUM(credit), 0) AS credit,
                COALESCE(sum(debit), 0) - COALESCE(sum(credit), 0) AS balance,
                COALESCE(sum(amount_currency), 0) AS curr_balance
              FROM account_move_line aml
              JOIN account_move am ON aml.move_id = am.id
                WHERE aml.period_id=%(period_id)s
                    {} -- AND aml.account_id IN
                    {} -- AND aml.journal_id IN
                    {} -- AND am.state = 'posted'
                    
              GROUP BY aml.period_id, aml.account_id, aml.journal_id
        """.format(
            'AND aml.account_id IN %(account_ids)s' if accounts else '',
            'AND aml.journal_id IN %(journal_ids)s' if journals else '',
            'AND am.state = \'posted\'' if not include_draft else '')

        self.env.cr.execute(query, {
            'period_id': period.id,
            'account_ids': tuple(accounts.ids) if accounts else None,
            'journal_ids': tuple(journals.ids) if journals else None,
        })
        time_taken = time() - start_time
        hours, rest = divmod(time_taken, 3600)
        minutes, seconds = divmod(rest, 60)

        logger.debug(
            "Completed calculation of balances for periods %s and %s "
            "accounts. Total duration: %.0f hours %.0f minutes %.0f seconds.",
            period.name, len(accounts) if accounts else '', hours, minutes,
            seconds)
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
                if not hasattr(res, str(values['account_id'])):
                    continue
                entry = res[values['account_id']]
                entry.update({
                    'credit': entry.get('credit', 0) + values['credit'],
                    'debit': entry.get('debit', 0) + values['debit'],
                    'balance': entry.get('balance', 0) + values['balance'],
                    'curr_balance': entry.get(
                        'curr_balance', 0) + values['curr_balance'],
                })

        # Get missing periods, accounts and journals to calc them on the fly
        missing_periods = self.get_missing_periods_accounts_and_journals(
            periods=periods)[0]

        if missing_periods:
            for period in missing_periods:
                map_data_to_account_id(
                    self.calculate_balance(
                        period, include_draft=include_draft))

        static_periods = self.env['account.static.balance'].search([
            ('period_id', 'in', periods.ids)
        ])
        static_periods = static_periods.mapped('period_id') - missing_periods

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
                    if (not hasattr(account, 'balance') or
                            not hasattr(entry, 'balance')):
                        # We have no calculated balance for this account_id
                        continue
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

    def _validate_integrity(self):
        """
        Validate the integrity from account_static balances, delete obsolete
        records immediately
        """
        clear_query = """
        DELETE FROM account_static_balance asb
            WHERE EXISTS (SELECT * FROM account_journal_period ajp
                          WHERE ajp.period_id = asb.period_id
                          AND ajp.journal_id = asb.journal_id
                          AND asb.create_date >= ajp.write_date
                          OR ajp.state != 'done')
            """
        self.env.cr.execute(clear_query)
