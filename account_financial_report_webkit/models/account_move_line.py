# -*- coding: utf-8 -*-
# © 2011 Camptocamp SA
# © 2016 Savoir-faire Linux
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from openerp import api, fields, models


class AccountMoveLine(models.Model):
    """
    Overriding Account move line in order to add last_rec_date.
    Last rec date is the date of the last reconciliation (full or partial)
    account move line
    """

    _inherit = 'account.move.line'

    last_rec_date = fields.Date(
        compute='_compute_last_rec_date',
        store=True,
        string='Last reconciliation date',
        help="The date of the last reconciliation (full or partial) "
        "account move line."
    )

    @api.depends(
        'reconcile_id.line_id.date',
        'reconcile_partial_id.line_partial_ids.date')
    def _compute_last_rec_date(self):
        for line in self:
            if line.reconcile_id:
                move_lines = line.reconcile_id.line_id
                last_line = move_lines.sorted(lambda l: l.date)[-1]
                line.last_rec_date = last_line.date

            elif line.reconcile_partial_id:
                move_lines = line.reconcile_partial_id.line_partial_ids
                last_line = move_lines.sorted(lambda l: l.date)[-1]
                line.last_rec_date = last_line.date

    @api.noguess
    def _auto_end(self, cr, context=None):
        """
        Create a combined index on account_move_line to handle performance
        on balance calculating. This index is here so the query to determine
        missing balances is performing.
        """
        cr.execute("""
            CREATE INDEX IF NOT EXISTS
                account_move_line_account_journal_period_index ON
                    account_move_line(account_id, journal_id, period_id);
        """)

        return super(AccountMoveLine, self)._auto_end(cr, context=context)
