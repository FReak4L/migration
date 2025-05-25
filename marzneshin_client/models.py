# migration/marzneshin_client/models.py

from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

# Import utility if needed for Pydantic models (e.g. for custom parsing within models)
# from utils.datetime_utils import iso_format_to_datetime 
# For now, keeping models simple. Datetime parsing is handled during transformation or API client layer if necessary.

# If logger is needed here (e.g., in validators), import it carefully
# from core.logging_setup import get_logger
# model_logger = get_logger()


class MarzneshinLoginToken(BaseModel):
    access_token: str
    token_type: str = Field(default="bearer")
    is_sudo: Optional[bool] = None

class MarzneshinAdmin(BaseModel):
    id: int
    username: str
    enabled: Optional[bool] = True
    is_sudo: Optional[bool] = False
    service_ids: List[int] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    model_config = {"from_attributes": True, "extra": "ignore"}

class MarzneshinAdminList(BaseModel):
    items: List[MarzneshinAdmin]
    total: int

class MarzneshinService(BaseModel):
    id: int
    name: str
    inbound_ids: List[int] = Field(default_factory=list)
    model_config = {"from_attributes": True, "extra": "ignore"}

class MarzneshinServiceList(BaseModel):
    items: List[MarzneshinService]
    total: int

class MarzneshinUserProxyDetail(BaseModel):
    id: str

class MarzneshinUserProxies(BaseModel):
    vless: Optional[MarzneshinUserProxyDetail] = None
    vmess: Optional[MarzneshinUserProxyDetail] = None
    trojan: Optional[MarzneshinUserProxyDetail] = None
    shadowsocks: Optional[MarzneshinUserProxyDetail] = None
    model_config = {"extra": "ignore"}

class MarzneshinUser(BaseModel):
    id: Optional[int] = None
    username: str
    key: Optional[str] = None
    proxies: MarzneshinUserProxies = Field(default_factory=MarzneshinUserProxies)
    enabled: Optional[bool] = True
    activated: Optional[bool] = None
    is_active: Optional[bool] = None
    expired: Optional[bool] = None
    data_limit_reached: Optional[bool] = None
    data_limit: Optional[int] = Field(default=0)
    used_traffic: Optional[int] = Field(default=0)
    lifetime_used_traffic: Optional[int] = Field(default=0)
    expire_date: Optional[datetime] = None
    expire_strategy: Optional[str] = None
    usage_duration: Optional[int] = None
    activation_deadline: Optional[datetime] = None
    data_limit_reset_strategy: Optional[str] = None
    traffic_reset_at: Optional[datetime] = None
    note: Optional[str] = None
    service_ids: List[int] = Field(default_factory=list)
    subscription_url: Optional[str] = None
    owner_username: Optional[str] = None
    created_at: Optional[datetime] = None
    sub_revoked_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    sub_updated_at: Optional[datetime] = None
    sub_last_user_agent: Optional[str] = None
    online_at: Optional[datetime] = None
    model_config = {"from_attributes": True, "extra": "ignore"}

class MarzneshinUserList(BaseModel):
    items: List[MarzneshinUser]
    total: int

class MarzneshinInbound(BaseModel):
    id: int
    tag: str
    protocol: str
    config: Union[Dict[str, Any], str]
    node_id: Optional[int] = None
    service_ids: List[int] = Field(default_factory=list)
    model_config = {"from_attributes": True, "extra": "ignore"}

    @field_validator("config", mode='before')
    @classmethod
    def ensure_config_is_dict(cls, v: Any) -> Any:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # Consider using a logger instance if more detailed logging is needed here
                # For now, print to stderr for simplicity if this model is used standalone
                print(f"Warning: Inbound config is a string but not valid JSON: '{v[:70]}...'. Using empty dict.", file=sys.stderr)
                return {}
        return v

class MarzneshinInboundList(BaseModel):
    items: List[MarzneshinInbound]
    total: int
