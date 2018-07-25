# -*- coding: utf-8 -*-
# © 2016 Savoir-faire Linux
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).
from psycopg2 import IntegrityError
from openerp.tests.common import SavepointCase
from openerp.tools.misc import mute_logger


class TestAccountStaticBalance(SavepointCase):

    @classmethod
    def setUpClass(cls):
        super(TestAccountStaticBalance, cls).setUpClass()
        # Clear existing entries (if any)
        cls.env['account.static.balance'].search([]).with_context(
            skip_recalculation=True).unlink()
        cls.period = cls.env['account.period'].search([], limit=1)
        cls.account = cls.env['account.account'].search([], limit=1)

    def test_uniqueness_constraint(self):
        """ We are not able to create double entries. """
        self.env['account.static.balance'].create({
            'account_id': self.account.id,
            'period_id': self.period.id
        })
        with mute_logger('openerp.sql_db'):
            with self.assertRaisesRegex(IntegrityError, "duplicate key value"):
                self.env['account.static.balance'].create({
                    'account_id': self.account.id,
                    'period_id': self.period.id
                })

    def test_run_cron(self):
        """ The cron method runs without any errors """
        self.env['account.static.balance'].auto_populate_table()
