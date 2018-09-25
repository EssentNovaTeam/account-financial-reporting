# -*- encoding: utf-8 -*-
##############################################################################
#
#    Author: Nicolas Bessi, Guewen Baconnier
#    Copyright Camptocamp SA 2011
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
from operator import itemgetter
from itertools import groupby
from datetime import datetime
from openerp.report import report_sxw
from openerp import pooler, api
from openerp.tools.translate import _
from .common_reports import CommonReportHeaderWebkit
from .webkit_parser_header_fix import HeaderFooterTextWebKitParser

LOGGER = logging.getLogger(__name__)


class GeneralLedgerWebkit(report_sxw.rml_parse, CommonReportHeaderWebkit):

    def __init__(self, cursor, uid, name, context):
        super(GeneralLedgerWebkit, self).__init__(
            cursor, uid, name, context=context)
        self.pool = pooler.get_pool(self.cr.dbname)
        self.cursor = self.cr

        company = self.pool.get('res.users').browse(
            self.cr, uid, uid, context=context).company_id
        header_report_name = ' - '.join(
            (_('GENERAL LEDGER'), company.name, company.currency_id.name))

        footer_date_time = self.formatLang(
            str(datetime.today()), date_time=True)

        self.localcontext.update({
            'cr': cursor,
            'uid': uid,
            'report_name': _('General Ledger'),
            'display_account': self._get_display_account,
            'display_account_raw': self._get_display_account_raw,
            'filter_form': self._get_filter,
            'target_move': self._get_target_move,
            'initial_balance': self._get_initial_balance,
            'amount_currency': self._get_amount_currency,
            'display_target_move': self._get_display_target_move,
            'accounts': self._get_accounts_br,
            'additional_args': [
                ('--header-font-name', 'Helvetica'),
                ('--footer-font-name', 'Helvetica'),
                ('--header-font-size', '10'),
                ('--footer-font-size', '6'),
                ('--header-left', header_report_name),
                ('--header-spacing', '2'),
                ('--footer-left', footer_date_time),
                ('--footer-right',
                 ' '.join((_('Page'), '[page]', _('of'), '[topage]'))),
                ('--footer-line',),
            ],
        })

    def set_context(self, objects, data, ids, report_type=None):
        """Populate a ledger_lines attribute on each browse record that will be
        used by mako template"""
        new_ids = data['form']['account_ids'] or data[
            'form']['chart_account_id']

        # Account initial balance memoizer
        init_balance_memoizer = {}

        # Reading form
        main_filter = self._get_form_param('filter', data, default='filter_no')
        target_move = self._get_form_param('target_move', data, default='all')
        start_date = self._get_form_param('date_from', data)
        stop_date = self._get_form_param('date_to', data)
        do_centralize = self._get_form_param('centralize', data)
        start_period = self.get_start_period_br(data)
        stop_period = self.get_end_period_br(data)
        fiscalyear = self.get_fiscalyear_br(data)
        chart_account = self._get_chart_account_id_br(data)

        if main_filter == 'filter_no':
            start_period = self.get_first_fiscalyear_period(fiscalyear)
            stop_period = self.get_last_fiscalyear_period(fiscalyear)

        # computation of ledger lines
        if main_filter == 'filter_date':
            start = start_date
            stop = stop_date
        else:
            start = start_period
            stop = stop_period

        initial_balance = self.is_initial_balance_enabled(main_filter)
        initial_balance_mode = initial_balance \
            and self._get_initial_balance_mode(start) or False

        # Retrieving accounts
        accounts = self.get_all_accounts(new_ids, exclude_type=['view'])
        if initial_balance_mode == 'initial_balance':
            init_balance_memoizer = self._compute_initial_balances(
                accounts, start, fiscalyear)
        elif initial_balance_mode == 'opening_balance':
            init_balance_memoizer = self._read_opening_balance(accounts, start)

        ledger_lines_memoizer = self.get_move_lines(
            accounts, main_filter, start, stop, target_move)
        objects = self.pool.get('account.account').browse(self.cursor,
                                                          self.uid,
                                                          accounts)

        init_balance = {}
        ledger_lines = {}
        for account in objects:
            if do_centralize and account.centralized \
                    and ledger_lines_memoizer.get(account.id):
                ledger_lines[account.id] = self._centralize_lines(
                    main_filter, ledger_lines_memoizer.get(account.id, []))
            else:
                ledger_lines[account.id] = ledger_lines_memoizer.get(
                    account.id, [])
            init_balance[account.id] = init_balance_memoizer.get(account.id,
                                                                 {})

        self.localcontext.update({
            'fiscalyear': fiscalyear,
            'start_date': start_date,
            'stop_date': stop_date,
            'start_period': start_period,
            'stop_period': stop_period,
            'chart_account': chart_account,
            'initial_balance_mode': initial_balance_mode,
            'init_balance': init_balance,
            'ledger_lines': ledger_lines,
        })

        return super(GeneralLedgerWebkit, self).set_context(
            objects, data, new_ids, report_type=report_type)

    def _centralize_lines(self, filter, ledger_lines, context=None):
        """ Group by period in filter mode 'period' or on one line in filter
            mode 'date' ledger_lines parameter is a list of dict built
            by _get_ledger_lines"""
        def group_lines(lines):
            if not lines:
                return {}
            sums = reduce(lambda line, memo:
                          dict((key, value + memo[key]) for key, value
                               in line.iteritems() if key in
                               ('balance', 'debit', 'credit')), lines)

            res_lines = {
                'balance': sums['balance'],
                'debit': sums['debit'],
                'credit': sums['credit'],
                'lname': _('Centralized Entries'),
                'account_id': lines[0]['account_id'],
            }
            return res_lines

        centralized_lines = []
        if filter == 'filter_date':
            # by date we centralize all entries in only one line
            centralized_lines.append(group_lines(ledger_lines))

        else:  # by period
            # by period we centralize all entries in one line per period
            period_obj = self.pool.get('account.period')
            # we need to sort the lines per period in order to use groupby
            # unique ids of each used period id in lines
            period_ids = list(
                set([line['lperiod_id'] for line in ledger_lines]))
            # search on account.period in order to sort them by date_start
            sorted_period_ids = period_obj.search(
                self.cr, self.uid, [('id', 'in', period_ids)],
                order='special desc, date_start', context=context)
            sorted_ledger_lines = sorted(
                ledger_lines, key=lambda x: sorted_period_ids.
                index(x['lperiod_id']))

            for period_id, lines_per_period_iterator in groupby(
                    sorted_ledger_lines, itemgetter('lperiod_id')):
                lines_per_period = list(lines_per_period_iterator)
                if not lines_per_period:
                    continue
                group_per_period = group_lines(lines_per_period)
                group_per_period.update({
                    'lperiod_id': period_id,
                    # period code is anyway the same on each line per period
                    'period_code': lines_per_period[0]['period_code'],
                })
                centralized_lines.append(group_per_period)

        return centralized_lines

    def get_move_lines(self, account_ids, main_filter, start, stop,
                       target_move, mode='include_opening'):
        """ Get all elegible move lines for all accounts.
        :param accounts_ids: account.account
        :param main_filter: string filter_no|filter_period|filter_date
        :param start: date|period
        :param stop: date|period
        :param target_move: string: select only posted moves
        :param mode: string: include_opening|exclude_opening
        """
        LOGGER.debug("GL Report: Building move line domain")
        self.env = api.Environment(self.cr, self.uid, {})
        account_map = {}

        # Ensure the right mode is specified
        if mode not in ('include_opening', 'excude_opening'):
            raise NotImplementedError(
                "Unknown mode specified. Mode can be either of: "
                "'include_opening' or 'exclude_opening'.")

        # Select the lowest possible IN for the accounts.
        excl_account_ids = self.env['account.account'].search(
            [('id', 'not in', account_ids)]).ids
        if len(excl_account_ids) < len(account_ids):
            domain = [('account_id', 'not in', excl_account_ids)]
        else:
            domain = [('account_id', 'in', account_ids)]

        if main_filter in ('filter_period', 'filter_no'):
            # Search for periods
            period_ids = self.env['account.period'].build_ctx_periods(
                start.id, stop.id)
            if not period_ids:
                # There is no period
                return account_map
            domain += [('period_id', 'in', period_ids)]

        if main_filter == 'filter_date':
            # Filter on date
            domain += [('date', '>=', start), ('date', '<=', stop)]

        if target_move == 'posted':
            # Only posted moves
            domain += [('move_id.state', '=', 'posted')]

        LOGGER.debug(
            "GL Report: Searching for move lines with domain: %s", domain)
        move_lines = self.env['account.move.line'].with_context(
            prefetch_fields=False).search(domain)

        # Construct the default results dictionary
        account_map = self.generate_empty_results(account_ids)

        if not move_lines:
            # There are no elegible move lines
            LOGGER.debug("GL report: No elegible move lines found")
            return account_map
        LOGGER.debug("GL Report: Collecting and mapping move line data")
        for line in self.chunked(move_lines.ids, model='account.move.line'):
            # Map the relevant line info to the correct dict key
            self.map_values_to_keys(account_map, line)

        return account_map

    @staticmethod
    def generate_empty_results(account_ids):
        """ Generate an empty dictionary with account id as keys and an empty
        list as value. """
        res = {}
        for account_id in account_ids:
            res[account_id] = []
        return res

    def map_values_to_keys(self, account_map, line):
        """ Gathers the data from each move line and inserts them into the
        account map. Ultimatly try to find the invoice id of the lines' move
        if there is any.
        :param account_map: Account map is dictionary mapping the account_id
                            to the relevant move lines
        :param line: the current move line we are evaluating
        :returns True, updated account_map
        """
        vals = {
            # Move line data
            'id': line.id,
            'ldate': line.date,
            'date_maturity': line.date_maturity,
            'amount_currency': line.amount_currency,
            'lref': line.ref,
            'lname': line.name,
            'balance': (line.debit or 0.0) - (line.credit or 0.0),
            'debit': line.debit,
            'credit': line.credit,
            # Move data
            'move_name': line.move_id.name,
            'move_id': line.move_id.id,
            # Journal data
            'jcode': line.journal_id.code,
            'jtype': line.journal_id.type,
            # Currency data
            'currency_id': line.currency_id.id,
            'currency_code': line.currency_id.name,
            # Account data
            'account_id': line.account_id.id,
            # Period data
            'lperiod_id': line.period_id.id,
            # Partner data
            'lpartner_id': line.partner_id.id,
            'partner_name': line.partner_id.name or '',
            # Reconcile data
            'rec_name': (line.reconcile_partial_id.name or
                         line.reconcile_id.name or ''),
            'rec_id': (line.reconcile_partial_id.id or
                       line.reconcile_id.id or False),
            # Default invoice values
            'invoice_id': False,
            'invoice_type': None,
            'invoice_number': None
        }

        # Get the invoice information
        if line.move_id:
            self.cr.execute("""
                SELECT id AS invoice_id,
                       type AS invoice_type,
                       number AS invoice_number
                FROM account_invoice
                WHERE move_id = %i
            """ % line.move_id.id)
            invoice_data = self.cr.dictfetchall()
            if invoice_data:
                vals.update(invoice_data[0])

        # Get the counterpart account information
        codes = line.mapped('move_id.line_id.account_id.code')
        sibling_codes = [c for c in codes if c != line.account_id.code]
        vals.update({'counterparts': ", ".join(sibling_codes)})

        # Insert the data of the line at the account_id key
        account_map[line.account_id.id] += [vals]


HeaderFooterTextWebKitParser(
    'report.account.account_report_general_ledger_webkit',
    'account.account',
    'addons/account_financial_report_webkit/report/templates/\
                                        account_report_general_ledger.mako',
    parser=GeneralLedgerWebkit)
