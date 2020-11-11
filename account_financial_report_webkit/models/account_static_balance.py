# coding: utf-8
# Copyright (C) 2018 DynApps <http://www.dynapps.be>
# @author Pieter Paulussen <pieter.paulussen@dynapps.be>
# @author Stefan Rijnhart <stefan.rijnhart@dynapps.nl>
# @author Robin Conjour <r.conjour@essent.be>
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).
from collections import defaultdict
import logging
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
        """ Calculates all possible combinations of journal-period-account
        for which no valid static balance is found.
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
                          AND ajp.state = 'done'))
                AND EXISTS (
                SELECT 1 FROM account_move_line aml
                WHERE aml.account_id = aa.id
                    AND aml.journal_id = aj.id
                    AND aml.period_id = ap.id)
                {};  -- optional period filter
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
        if self and not self.env.context.get('skip_recalculation', False):
            # Trigger recalculation of the entry
            self.env['account.static.balance'].calculate_static_balance(
                periods=periods, accounts=accounts, journals=journals)
        return res

    @api.model
    def auto_populate_table(self):
        """ ir.cron function to trigger intial balance calculation for all
        closed periods and accounts if their combination is missing. """
        # Remove static balances with integrity errors
        self.env.cr.execute("""
            DELETE FROM account_static_balance asb
            WHERE EXISTS (SELECT * FROM account_journal_period ajp
                          WHERE ajp.period_id = asb.period_id
                          AND ajp.journal_id = asb.journal_id
                          AND (ajp.write_date >= asb.create_date
                               OR ajp.state != 'done'))
        """)
        period_journal_map = self.get_missing_periods_accounts_and_journals()
        if not period_journal_map:
            logger.debug("No missing entries found. Static account balance "
                         "data is up to date.")
            return

        for period, journals in period_journal_map.items():
            self.calculate_static_balance(period, journals=journals)

    @api.multi
    def get_missing_periods_accounts_and_journals(self, periods=None):
        # Split into unique periods and accounts
        missing_combinations = self.get_entries_to_calculate(periods=periods)
        period_journal_map = defaultdict(lambda: self.env['account.journal'])
        for period_id, _account_id, journal_id in missing_combinations:
            period_journal_map[
                self.env['account.period'].browse(period_id)] |= (
                    self.env['account.journal'].browse(journal_id))
        return period_journal_map

    @api.model
    def calculate_static_balance(self, periods, accounts=None, journals=None):
        """ Calculate the balance for a specified set of periods
        and insert them into the table."""
        self = self.suspend_security()

        if not journals:
            # When journals are set, we can assume that this is a close for
            # a journal period. Otherwise we need to filter the periods to
            # check if they are in a done state
            periods = periods.filtered(lambda p: p.state == 'done')
        periods = periods.filtered(lambda p: not p.special)
        if not periods:
            return

        for period in periods:
            logging.info(
                'Storing the calculated static balances for period %s, '
                'journals %s and accounts %s',
                period.name,
                ','.join(journals.mapped('name') if journals else []) or '-',
                ','.join(accounts.mapped('code') if accounts else []) or '-')
            domain = [('period_id', '=', period.id)]
            if journals:
                domain.append(('journal_id', 'in', journals.ids))
            if accounts:
                domain.append(('account_id', 'in', accounts.ids))
            self.env['account.static.balance'].with_context(
                skip_recalculation=True).search(domain).unlink()
            for row in self.calculate_balance(
                    period, journals=journals, accounts=accounts):
                self.create(row)

    @api.model
    def calculate_balance(
            self, period, journals=None, accounts=None, include_draft=False):
        """
        Calculate the balance for a specified set of periods
        :param period: period to calculate the values for
        :param accounts: optional accounts to calculate the values for
        :param journals: optional journals to calculate the values for
        :return: True
        """
        period.ensure_one()

        logger.debug(
            "Starting calculation of balances for "
            "period %s, accounts %s and journals %s",
            period.name,
            ','.join(a.code for a in accounts or []) or '-',
            ','.join(j.code for j in journals or []) or '-')

        start_time = time()

        # Prevent adding a meaningless filter on journal_id
        if journals:
            self.env.cr.execute(
                """ SELECT journal_id FROM account_journal_period ajp
                WHERE period_id = %s """, (period.id,))
            all_journal_ids = [
                journal_id for journal_id, in self.env.cr.fetchall()]
            if set(journals.ids) >= set(all_journal_ids):
                journals = None

        # Prevent adding a meaningless filter on account_id
        if accounts:
            if not self.env['account.account'].with_context(
                    active_test=False).search([
                        ('id', 'not in', accounts.ids), ('type', '!=', 'view'),
                        ('company_id', '=', period.company_id.id),
                    ]):
                accounts = None

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
            "Completed calculation of balances for period %s and %s "
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
        res = {}
        if not accounts or not periods:
            return res

        return_ids = accounts.ids  # Don't return child and consolitated
        if consolidate:
            accounts |= self.env['account.account'].browse(
                accounts._get_children_and_consol())

        fields_to_read = [
            'id', 'type', 'code', 'name', 'parent_id', 'level', 'child_id']
        for account in accounts.read(fields_to_read):
            account.update({
                'credit': 0,
                'debit': 0,
                'balance': 0,
                'curr_balance': 0,
            })
            res[account['id']] = account

        def map_data_to_account_id(data):
            for values in data:
                if values['account_id'] not in accounts.ids:
                    continue
                entry = res[values['account_id']]
                entry.update({
                    'credit': entry['credit'] + values['credit'],
                    'debit': entry['debit'] + values['debit'],
                    'balance': entry['balance'] + values['balance'],
                    'curr_balance': (entry['curr_balance'] +
                                     values['curr_balance']),
                })

        # Get missing periods, accounts and journals to calc them on the fly
        for period, journals in self.get_missing_periods_accounts_and_journals(
                periods=periods).items():
            map_data_to_account_id(
                self.calculate_balance(
                    period, journals=journals, accounts=accounts,
                    include_draft=include_draft))

        # Get amounts from valid static balances
        self.env.cr.execute(
            """
            SELECT account_id,
                SUM(credit) AS credit,
                SUM(debit) AS debit,
                SUM(balance) as balance,
                SUM(curr_balance) AS curr_balance
            FROM account_static_balance asb
            WHERE EXISTS (
                SELECT * FROM account_journal_period ajp
                WHERE ajp.period_id = asb.period_id
                    AND ajp.journal_id = asb.journal_id
                    AND ajp.write_date < asb.create_date
                    AND ajp.state = 'done')
                AND asb.period_id IN %(period_ids)s
            GROUP BY account_id
            """, {'period_ids': tuple(periods.ids)})
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
                    account.update({
                        'credit': account['credit'] + entry['credit'],
                        'debit': account['debit'] + entry['debit'],
                        'balance': account['balance'] + entry['balance'],
                        'curr_balance': (account['curr_balance'] +
                                         entry['curr_balance']),
                    })
        return dict(item for item in res.iteritems() if item[0] in return_ids)
