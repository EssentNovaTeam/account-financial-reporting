# coding: utf-8
import logging
import time

logger = logging.getLogger(__name__)

# Query to recalculate static balances
query = """
        INSERT INTO account_static_balance (
        create_uid, create_date, account_id, curr_balance, write_uid, credit,
        period_id, write_date, debit, balance, fiscalyear_id, journal_id) (
        WITH balances AS (
              -- Calculate current account, journal, period combinations
              SELECT
                aml.period_id AS period_id,
                aml.account_id AS account_id,
                aml.journal_id AS journal_id,
                ap.fiscalyear_id AS fiscalyear_id,
                COALESCE(SUM(debit), 0) AS debit,
                COALESCE(SUM(credit), 0) AS credit,
                COALESCE(sum(debit), 0) - COALESCE(sum(credit), 0) AS balance,
                COALESCE(sum(amount_currency), 0) AS curr_balance
              FROM account_move_line aml
              JOIN account_period ap ON ap.id = aml.period_id
              JOIN account_move am ON aml.move_id = am.id
                WHERE ap.state = 'done'
              GROUP BY aml.period_id, aml.account_id, aml.journal_id,
              ap.fiscalyear_id)
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
              fiscalyear_id AS fiscalyear_id,
              journal_id AS journal_id
              FROM balances
            GROUP BY period_id, account_id, journal_id, fiscalyear_id);
            """


def migrate(cr, version):
    """ Generate new static balances for closed periods """
    if not version:
        return

    # Generating new static balances for closed periodes
    logger.info(
        'Calculate balance again grouped by journal_ids.')
    start = time.time()
    cr.execute(query)
    elapsed_time = time.time() - start

    # We are done
    logger.info('Recalculation of balances took %s', elapsed_time)
