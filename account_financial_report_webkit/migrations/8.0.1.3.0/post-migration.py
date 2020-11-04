# coding: utf-8
from openupgradelib import openupgrade
import logging
import time

logger = logging.getLogger(
    'openerp.addons.account_financial_report_webkit.migrations.8.0.1.3.0'
    '.post-migration')

# Query to recalculate static balances
query = """
        INSERT INTO account_static_balance (
        create_uid, create_date, account_id, curr_balance, write_uid, credit,
        period_id, write_date, debit, balance, journal_id) (
        WITH balances AS (
              -- Calculate current account, journal, period combinations
              SELECT
                aml.period_id AS period_id,
                aml.account_id AS account_id,
                aml.journal_id AS journal_id,
                COALESCE(SUM(debit), 0) AS debit,
                COALESCE(SUM(credit), 0) AS credit,
                COALESCE(sum(debit), 0) - COALESCE(sum(credit), 0) AS balance,
                COALESCE(sum(amount_currency), 0) AS curr_balance
              FROM account_move_line aml
              JOIN account_period ap ON ap.id = aml.period_id
              JOIN account_move am ON aml.move_id = am.id
                WHERE ap.state = 'done'
              GROUP BY aml.period_id, aml.account_id, aml.journal_id)
            SELECT
              -- Follow structure of account_static_balance
              1 AS create_uid,
              NOW() AS create_date,
              account_id AS account_id,
              sum(curr_balance) as curr_balance,
              1 AS write_uid,
              SUM(credit) AS credit,
              period_id AS period_id,
              NOW() AS write_date,
              SUM(debit) AS debit,
              sum(balance) as balance,
              journal_id AS journal_id
              FROM balances
            GROUP BY period_id, account_id, journal_id);
            """


def migrate(cr, version):
    """ Generate new static balances for closed periods """
    if not version:
        return

    logger.info(
        'Inserting missing journal periods for migrated entries')
    start = time.time()
    cr.execute(
        """
        INSERT INTO account_journal_period
        (create_date, write_date, create_uid, write_uid, active,
        company_id, period_id, journal_id, state, name)
        SELECT DISTINCT NOW() AT TIME ZONE 'UTC',
            NOW() AT TIME ZONE 'UTC', 1, 1, TRUE,
            ap.company_id, ap.id, am.journal_id,
            CASE WHEN ap.state IN ('done', 'printed') THEN 'done'
                 ELSE 'draft'
            END,
            aj.code || ';' || ap.name
        FROM account_move am
        JOIN account_period ap ON ap.id = am.period_id
        JOIN account_journal aj ON aj.id = am.journal_id
        WHERE NOT EXISTS (
            SELECT * FROM account_journal_period ajp
            WHERE ajp.period_id = am.period_id
                AND ajp.journal_id = am.journal_id);
        """)
    if openupgrade.column_exists(
            cr, 'account_journal_period', 'journal_type'):
        cr.execute(
            """ UPDATE account_journal_period ajp
            SET journal_type = aj.type
            FROM account_journal aj WHERE aj.id = ajp.journal_id
                AND ajp.journal_type IS NULL """)

    elapsed_time = time.time() - start
    logger.info(
        'Inserted %s missing journal periods for migrated entries in %s '
        'seconds', cr.rowcount, elapsed_time)

    # Generating new static balances for closed periodes
    start = time.time()
    logger.info(
        'Calculate balance again grouped by journal_ids.')
    cr.execute(query)
    elapsed_time = time.time() - start

    # We are done
    logger.info('Recalculation of balances took %s', elapsed_time)
