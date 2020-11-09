# -*- coding: utf-8 -*-
# © 2019-2020 Essent
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).
import imp
import os
from psycopg2 import IntegrityError
from openerp.modules import get_module_resource
from openerp.tests.common import SavepointCase
from openerp.tools.misc import file_open, mute_logger


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

    def test_04_migrations(self):
        """ Test the migration of the introduction of journal_id column
        in the static balance data model
        """
        def run_migration(script):
            pyfile = get_module_resource(
                'account_financial_report_webkit', 'migrations',
                '8.0.1.3.0', script)
            name, ext = os.path.splitext(os.path.basename(pyfile))
            fp, pathname = file_open(pyfile, pathinfo=True)
            mod = imp.load_module(
                name, fp, pathname, ('.py', 'r', imp.PY_SOURCE))
            mod.migrate(self.env.cr, '8.0.1.3.0')

        run_migration('pre-migration.py')
        run_migration('post-migration.py')
