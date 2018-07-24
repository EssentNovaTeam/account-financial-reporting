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
from openerp import api, models


class AccountPeriodClose(models.TransientModel):
    _inherit = 'account.period.close'

    @api.multi
    def data_save(self):
        """ Create static account credit, debit and balance data when closing a
        period to speed up the reporting output."""
        res = super(AccountPeriodClose, self).data_save()

        periods = self.env['account.period'].browse(
            self.env.context.get('active_ids', []))
        self.env['account.static.balance'].calculate_static_balance(periods)
        return res
