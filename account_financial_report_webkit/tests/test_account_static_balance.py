# -*- coding: utf-8 -*-
# © 2016 Savoir-faire Linux
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).
from datetime import datetime, timedelta
from psycopg2 import IntegrityError
from openerp.tests.common import SavepointCase
from openerp.tools.misc import mute_logger


class TestAccountStaticBalance(SavepointCase):

    @classmethod
    def setUpClass(cls):
        super(TestAccountStaticBalance, cls).setUpClass()
        # Clear existing entries (if any)
        cls.env['ir.rule']._register_hook()  # Enable suspend_security
        cls.env['account.static.balance'].search([]).with_context(
            skip_recalculation=True).unlink()

        cls.period = cls.env['account.period'].search([], limit=1)
        cls.account = cls.env['account.account'].search([], limit=1)
        cls.journal = cls.env['account.journal'].search([], limit=1)

        cls.account_expense = cls.env['account.account'].search([
            ('type', '=', 'other'),
        ], limit=1)

        cls.account_receivable = cls.env['account.account'].search([
            ('type', '=', 'receivable'),
        ], limit=1)

        cls.journal = cls.env['account.journal'].search([
            ('type', '=', 'bank'),
        ], limit=1)

        cls.move_1 = cls.env['account.move'].create({
            'name': '/',
            'journal_id': cls.journal.id,
            'period_id': cls.period.id,
            'line_id': [
                (0, 0, {
                    'name': '/',
                    'account_id': cls.account_receivable.id,
                    'debit': 100,
                }),
                (0, 0, {
                    'name': '/',
                    'account_id': cls.account_expense.id,
                    'credit': 100,
                }),
            ]
        })

        # Post move
        cls.move_1.post()

        cls.env['account.journal.period'].create({
            'period_id': cls.period.id,
            'journal_id': cls.journal.id,
            'name': 'Journal period'
        })

    def test_01_uniqueness_constraint(self):
        """ We are not able to create double entries. """
        self.env['account.static.balance'].create({
            'account_id': self.account.id,
            'period_id': self.period.id,
            'journal_id': self.journal.id
        })
        with mute_logger('openerp.sql_db'):
            with self.assertRaisesRegex(IntegrityError, "duplicate key value"):
                self.env['account.static.balance'].create({
                    'account_id': self.account.id,
                    'period_id': self.period.id,
                    'journal_id': self.journal.id
                })

    def test_02_run_cron(self):
        """ The cron method runs without any errors """
        self.env['account.static.balance'].auto_populate_table()

    def test_03_calculate_balance(self):
        """ Trigger the calculate balance. """

        # Close all account_journal_periods
        self.env.cr.execute("""
            UPDATE account_journal_period SET state='done' WHERE period_id=%s
        """, (self.period.id,))

        # Close account_period
        self.env.cr.execute("""
            UPDATE account_period SET state='done', special='f' WHERE id=%s
        """, (self.period.id,))

        self.env['account.static.balance'].calculate_static_balance(
            self.period)
        balances = self.env['account.static.balance'].search([])

        # There will be only one balance because this period contains only 1
        # move with lines on two different accounts
        self.assertEqual(len(balances), 2)

    def test_04_calculate_balance_for_journal(self):
        """ Trigger the calculate balance for a specific journal. """

        # Create move in other journal, so we can validate if there is only
        # a balance generated for this journal
        journal_general = self.env['account.journal'].search([
            ('type', '=', 'general'),
        ], limit=1)

        move = self.env['account.move'].create({
            'name': '/',
            'journal_id': journal_general.id,
            'period_id': self.period.id,
            'line_id': [
                (0, 0, {
                    'name': '/',
                    'account_id': self.account_receivable.id,
                    'debit': 100,
                }),
                (0, 0, {
                    'name': '/',
                    'account_id': self.account_expense.id,
                    'credit': 100,
                }),
            ]
        })

        # Post move
        move.post()

        self.env['account.static.balance'].calculate_static_balance(
            self.period, journals=journal_general)

        balances = self.env['account.static.balance'].search([])

        # The amount of static balances should be equal to the amount of
        # unique period - journal - account combinations
        self.assertEqual(len(balances), 2)
        self.assertEqual(balances.mapped('period_id'), self.period)
        self.assertEqual(balances.mapped('journal_id'), journal_general)

    def test_05_invalidated_journal(self):
        """ Trigger invalidated journals. """
        # Create move in other journal, so we can validate if there is only
        # a balance generated for this journal
        journal_general = self.env['account.journal'].search([
            ('type', '=', 'general'),
        ], limit=1)

        move = self.env['account.move'].create({
            'name': '/',
            'journal_id': journal_general.id,
            'period_id': self.period.id,
            'line_id': [
                (0, 0, {
                    'name': '/',
                    'account_id': self.account_receivable.id,
                    'debit': 100,
                }),
                (0, 0, {
                    'name': '/',
                    'account_id': self.account_expense.id,
                    'credit': 100,
                }),
            ]
        })

        # Post move
        move.post()

        self.env['account.static.balance'].calculate_static_balance(
            self.period, journals=journal_general)

        balances = self.env['account.static.balance'].search([])

        # The amount of static balances should be equal to the amount of
        # unique period - journal - account combinations
        self.assertEqual(len(balances), 2)
        self.assertEqual(balances.mapped('period_id'), self.period)
        self.assertEqual(balances.mapped('journal_id'), journal_general)
        create_date = min(balances.mapped('create_date'))

        yesterday = datetime.now() - timedelta(days=1)
        self.env.cr.execute("""
            UPDATE account_static_balance SET create_date=%s
                WHERE period_id = %s
        """, (yesterday, self.period.id,))
        balances.refresh()

        # Validate that the date is updated succesfully
        self.assertEqual(
            str(yesterday)[:-7], max(balances.mapped('create_date')))

        # Calculate balance again, but we know that the integrity of the
        # static balance is not ok

        self.env['account.static.balance'].calculate_static_balance(
            self.period, journals=journal_general)
        new_balances = self.env['account.static.balance'].search([])

        # Validate that the balances are recalculated
        self.assertNotEqual(balances, new_balances)
        self.assertEqual(len(balances), len(new_balances))
