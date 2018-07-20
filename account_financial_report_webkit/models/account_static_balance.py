# -*- encoding: utf-8 -*-
##############################################################################
#
#    Author: Nicolas Bessi, Guewen Baconnier
#    Copyright Camptocamp SA 2011
#    SQL inspired from OpenERP original code
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################
import logging
import psycopg2
from time import time
from openerp import api, fields, models
from openerp.exceptions import Warning as UserError

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

    @api.model
    def check_data_ready(self):
        """ Override me to signal static data is ready or not. """
        return True

    @api.multi
    def get_entries_to_calculate(self):
        """ Calculates all possible combinations of accounts and closed
        periods which are missing from the expected result.
        """
        self.env.cr.execute("""
          WITH balances AS (
            SELECT ap.id as period, aa.id as account, asb.id as balance
            FROM account_period ap
            CROSS JOIN account_account aa
            LEFT JOIN account_static_balance asb
              ON asb.account_id = aa.id AND asb.period_id = ap.id
            WHERE ap.state = 'done')
          SELECT period, account FROM balances WHERE balance IS NULL;
        """)
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
        missing_combinations = self.get_entries_to_calculate()

        if not missing_combinations:
            logger.debug("No missing entries found. Static account balance "
                         "data is up to date.")
            return
        # Split into unique periods and accounts
        per_ids, acc_ids = map(list, map(set, zip(*missing_combinations)))

        missing_periods = self.env['account.period'].browse(per_ids)
        missing_accounts = self.env['account.account'].browse(acc_ids)

        self.calculate_static_balance(
            periods=missing_periods, accounts=missing_accounts)

    @api.model
    def calculate_static_balance(self, periods, accounts=None):
        """
        Calculate the balance for a specified set of periods and insert them
        into the table.
        :param periods: periods to calculate the values for
        :param accounts: optional accounts to calculate the values for
        :return: True
        """
        if any([state != 'done' for state in periods.mapped('state')]):
            raise UserError("Cannot calculate static balance for a period "
                            "that is not closed.")

        if accounts is None:
            # Calculate for all accounts
            accounts = self.env['account.account'].search([])

        logger.debug(
            "Starting calculation of static balances for closed "
            "periods: %s.", ", ".join(periods.mapped('name')))

        start_time = time()

        # Calculate the values for the periods
        self.env.cr.execute("""
            SELECT
              ap.id AS period_id,
              aa.id AS account_id,
              COALESCE(SUM(debit), 0) AS debit,
              COALESCE(SUM(credit), 0) AS credit,
              COALESCE(sum(debit), 0) - COALESCE(sum(credit), 0) AS balance,
              COALESCE(sum(amount_currency), 0) AS curr_balance
            FROM account_account aa
            CROSS JOIN account_period ap
            LEFT JOIN account_move_line aml 
              ON aa.id = account_id AND ap.id = period_id
            WHERE ap.state = 'done'
              AND ap.id IN %s
              AND aa.id IN %s
            GROUP BY ap.id, aa.id 
            ORDER BY ap.id, aa.id
        """, (tuple(periods.ids), tuple(accounts.ids)))

        # Map the results
        for row in self.env.cr.dictfetchall():
            try:
                with self.env.cr.savepoint():
                    self.create(row)
            except psycopg2.IntegrityError:
                pass

        time_taken = time() - start_time
        hours, rest = divmod(time_taken, 3600)
        minutes, seconds = divmod(rest, 60)
        logger.debug(
            "Completed calculation of static balance for periods: %s. "
            "Total duration: %.0f hours %.0f minutes %.0f seconds.",
            ", ".join(periods.mapped('name')), hours, minutes, seconds)
