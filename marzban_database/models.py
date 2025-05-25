# migration/marzban_database/models.py

from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Text, JSON,
    BigInteger, UniqueConstraint, Index, func
)
from datetime import datetime, timezone # Added for server_default=func.now() if needed, though usually SA handles it

MarzbanBase = declarative_base()

class MarzbanUser(MarzbanBase):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    username = Column(String(128), unique=True, index=True, nullable=False)
    status = Column(String(32), default="active", nullable=False)
    used_traffic = Column(BigInteger, default=0, nullable=False)
    data_limit = Column(BigInteger, default=0, nullable=False)
    expire = Column(BigInteger, nullable=True) # Unix timestamp (UTC)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    admin_id = Column(Integer, ForeignKey("admins.id", name="fk_mzb_users_admin_id"), nullable=True, index=True)
    data_limit_reset_strategy = Column(String(32), default="no_reset", nullable=False)
    note = Column(Text, nullable=True)
    sub_revoked_at = Column(DateTime(timezone=True), nullable=True)
    sub_updated_at = Column(DateTime(timezone=True), nullable=True)
    sub_last_user_agent = Column(String(255), nullable=True)
    online_at = Column(DateTime(timezone=True), nullable=True)
    edit_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    on_hold_timeout = Column(DateTime(timezone=True), nullable=True)
    on_hold_expire_duration = Column(Integer, nullable=True)
    auto_delete_in_days = Column(Integer, nullable=True)
    last_status_change = Column(DateTime(timezone=True), nullable=True, server_default=func.now())
    __table_args__ = (UniqueConstraint('username', name='uq_mzb_users_username'),)

class MarzbanProxy(MarzbanBase):
    __tablename__ = "proxies"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", name="fk_mzb_proxies_user_id", ondelete="CASCADE"), nullable=False, index=True)
    type = Column(String(32), nullable=False)
    settings = Column(JSON, nullable=False)
    __table_args__ = (Index('idx_mzb_proxies_user_id_type', 'user_id', 'type'), )

class MarzbanAdmin(MarzbanBase):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    username = Column(String(128), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    is_sudo = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    password_reset_at = Column(DateTime(timezone=True), nullable=True)
    telegram_id = Column(BigInteger, nullable=True, unique=True, index=True)
    discord_webhook = Column(Text, nullable=True)
    users_usage = Column(BigInteger, nullable=True, server_default='0')
    __table_args__ = (UniqueConstraint('username', name='uq_mzb_admins_username'),)
