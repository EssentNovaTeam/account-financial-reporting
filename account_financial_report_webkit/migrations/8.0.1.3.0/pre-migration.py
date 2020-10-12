# coding: utf-8
import logging

logger = logging.getLogger(__name__)


def migrate(cr, version):
    """
    Truncate account_static_balance, this will be recalculated in the
    post-migration
    """
    if not version:
        return

    # Clearing existing static balances
    logger.info(
        'Remove existing static balances.')
    cr.execute("TRUNCATE TABLE account_static_balance;")
    logger.info(
        'Done removing existing static balances.')

    logger.info("Drop constraint")
    cr.execute("""
        ALTER TABLE account_static_balance 
        DROP CONSTRAINT account_static_balance_account_static_balance_unique;
        """)

    logger.info("Dropped constraint")

