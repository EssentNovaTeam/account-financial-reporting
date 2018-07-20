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
from openerp import api, fields, models


class AccountPeriod(models.Model):
    _inherit = 'account.period'

    @api.multi
    def action_draft(self):
        """ When reopening a period, we need to clear the results from the
        static balance table. """
        res = super(AccountPeriod, self).action_draft()

        # Trigger static data removal without recalculation
        self.env['account.static.balance'].with_context(
            skip_recalculation=True).search(
                [('period_id', 'in', self.ids)]).unlink()

        return res
