"""Switched drs_id to file_id, made size nullable

Revision ID: dda104bd723f
Revises: 7854bc67c695
Create Date: 2021-12-09 08:30:53.586416

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "dda104bd723f"
down_revision = "7854bc67c695"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("drs_objects", "external_id", new_column_name="file_id")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("drs_objects", "file_id", new_column_name="external_id")
    # ### end Alembic commands ###
