# migration/transformer/data_transformer.py

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.config import AppConfig
from core.logging_setup import get_logger, get_console, RICH_AVAILABLE, RichText # type: ignore
from core.fault_tolerance import (
    fault_tolerance_manager, fault_tolerant, 
    RetryConfig, FallbackConfig
)
from utils.datetime_utils import iso_format_to_datetime, datetime_to_unix_timestamp, unix_timestamp_to_datetime

if RICH_AVAILABLE:
    from rich.table import Table
else: # Dummy Table if Rich is not available
    class Table: # type: ignore
        def __init__(self, *args, **kwargs): self.title = kwargs.get("title"); self._rows = []
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): self._rows.append(args)
        def __str__(self):
            header = f"{self.title}\n" if self.title else ""
            return header + "\n".join([" | ".join(map(str,r)) for r in self._rows])


logger = get_logger()
console = get_console()

class DataTransformer:
    """Transforms data from Marzneshin format to Marzban format."""
    def __init__(self, config: AppConfig):
        self.config = config
        
        # Initialize fault tolerance
        self.fault_tolerance = fault_tolerance_manager
        self._initialize_fault_tolerance()

    def _initialize_fault_tolerance(self):
        """Initialize fault tolerance configurations and health checks."""
        # Configure fallback values for common transformation scenarios
        fallback_config = FallbackConfig(
            enable_cache_fallback=True,
            cache_ttl=300,  # 5 minutes
            enable_default_values=True,
            default_values={
                'user_status': 'active',
                'admin_status': True,
                'proxy_type': 'VLESS',
                'data_limit': 0,
                'expire_date': None
            }
        )
        
        # Update fallback strategy
        self.fault_tolerance.fallback_strategy.config = fallback_config
        
        # Register health checks for external services
        self._register_health_checks()
        
        logger.info("Fault tolerance initialized for DataTransformer")
    
    def _register_health_checks(self):
        """Register health checks for external services."""
        # Note: These would be actual health check functions in a real implementation
        async def database_health_check():
            """Check database connectivity."""
            # Placeholder for actual database health check
            return True
        
        async def api_health_check():
            """Check API connectivity."""
            # Placeholder for actual API health check
            return True
        
        # Register health checks
        self.fault_tolerance.register_service_health_check(
            "database", database_health_check
        )
        self.fault_tolerance.register_service_health_check(
            "api", api_health_check
        )

    def _convert_key_to_uuid_format(self, key_str: str) -> Optional[str]:
        """Converts a 32-character hex string to standard UUID format with dashes."""
        if isinstance(key_str, str) and len(key_str) == 32:
            try:
                # Basic validation that it's a hex string
                int(key_str, 16)
                # Format to XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
                return f"{key_str[:8]}-{key_str[8:12]}-{key_str[12:16]}-{key_str[16:20]}-{key_str[20:]}"
            except ValueError:
                logger.warning(f"Invalid characters in key for UUID formatting: {key_str}")
                return None
        logger.debug(f"Key '{key_str}' is not a 32-char string; cannot format to UUID.")
        return None

    def _determine_mzb_user_status(self, mzsh_user_dict: Dict[str, Any]) -> str:
        """Determines Marzban user status based on Marzneshin user data."""
        if mzsh_user_dict.get("enabled") is False:
            return "disabled"
        expire_dt_obj = iso_format_to_datetime(mzsh_user_dict.get("expire_date"))
        if mzsh_user_dict.get("expired") is True or \
           (isinstance(expire_dt_obj, datetime) and expire_dt_obj < datetime.now(timezone.utc)):
            return "expired"
        if mzsh_user_dict.get("expire_strategy") == "start_on_first_use" and \
           mzsh_user_dict.get("activated") is False and \
           iso_format_to_datetime(mzsh_user_dict.get("activation_deadline")) and \
           iso_format_to_datetime(mzsh_user_dict.get("activation_deadline")) > datetime.now(timezone.utc): # type: ignore
            return "on_hold"
        data_limit = mzsh_user_dict.get("data_limit", 0) or 0
        used_traffic = mzsh_user_dict.get("used_traffic", 0) or 0
        if data_limit > 0 and used_traffic >= data_limit:
            if mzsh_user_dict.get("data_limit_reached") is True:
                 return "limited"
        return "active"

    def transform_user_for_marzban(self, mzsh_user_dict: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        username = mzsh_user_dict.get("username")
        if not username:
            if logger.getEffectiveLevel() <= logging.DEBUG: logger.debug(f"Skipping Marzneshin user with ID {mzsh_user_dict.get('id')} due to missing username.")
            return None, []

        mzb_user_data: Dict[str, Any] = {
            "username": username,
            "status": self._determine_mzb_user_status(mzsh_user_dict),
            "data_limit": mzsh_user_dict.get("data_limit", 0) or 0,
            "used_traffic": mzsh_user_dict.get("used_traffic", 0) or 0,
            "expire": datetime_to_unix_timestamp(iso_format_to_datetime(mzsh_user_dict.get("expire_date"))),
            "created_at": iso_format_to_datetime(mzsh_user_dict.get("created_at")) or datetime.now(timezone.utc),
            "edit_at": iso_format_to_datetime(mzsh_user_dict.get("updated_at")) or datetime.now(timezone.utc),
            "note": mzsh_user_dict.get("note"),
            "sub_revoked_at": iso_format_to_datetime(mzsh_user_dict.get("sub_revoked_at")),
            "sub_updated_at": iso_format_to_datetime(mzsh_user_dict.get("sub_updated_at")),
            "sub_last_user_agent": mzsh_user_dict.get("sub_last_user_agent"),
            "online_at": iso_format_to_datetime(mzsh_user_dict.get("online_at")),
            "data_limit_reset_strategy": mzsh_user_dict.get("data_limit_reset_strategy", "no_reset") or "no_reset",
            "admin_id": None,
            "on_hold_timeout": None,
            "on_hold_expire_duration": None,
            "auto_delete_in_days": None,
            "last_status_change": datetime.now(timezone.utc)
        }

        if mzb_user_data["status"] == "on_hold":
            mzb_user_data["on_hold_timeout"] = iso_format_to_datetime(mzsh_user_dict.get("activation_deadline"))
            mzb_user_data["on_hold_expire_duration"] = mzsh_user_dict.get("usage_duration")
            mzb_user_data["expire"] = None

        if mzb_user_data["edit_at"] < mzb_user_data["created_at"]: # type: ignore
             mzb_user_data["edit_at"] = mzb_user_data["created_at"]

        mzb_proxies_list: List[Dict[str, Any]] = []
        user_key_as_main_uuid = mzsh_user_dict.get("key")
        proxies_from_mzsh_api = mzsh_user_dict.get("proxies", {})
        found_explicit_proxy_uuid = False

        if isinstance(proxies_from_mzsh_api, dict) and proxies_from_mzsh_api:
            for protocol_name_mzsh, proxy_detail_mzsh in proxies_from_mzsh_api.items():
                if isinstance(proxy_detail_mzsh, dict) and proxy_detail_mzsh.get("id"):
                    proxy_settings_for_mzb = {"id": proxy_detail_mzsh["id"]}
                    mzb_proxies_list.append({
                        "_username_ref": username,
                        "type": protocol_name_mzsh.upper(),
                        "settings": json.dumps(proxy_settings_for_mzb)
                    })
                    found_explicit_proxy_uuid = True

        if not found_explicit_proxy_uuid and user_key_as_main_uuid:
            formatted_uuid = self._convert_key_to_uuid_format(user_key_as_main_uuid)
            if formatted_uuid:
                logger.debug(f"User '{username}' using formatted user.key '{formatted_uuid}' as fallback VLESS UUID.")
                mzb_proxies_list.append({
                    "_username_ref": username,
                    "type": "VLESS",
                    "settings": json.dumps({"id": formatted_uuid})
                })
            else:
                logger.warning(f"User '{username}' fallback key '{user_key_as_main_uuid}' could not be formatted to UUID. Proxy entry from key will be skipped.")

        return mzb_user_data, mzb_proxies_list

    def transform_admin_for_marzban(self, mzsh_admin_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        username = mzsh_admin_dict.get("username")
        if not username:
            if logger.getEffectiveLevel() <= logging.DEBUG: logger.debug(f"Skipping Marzneshin admin with ID {mzsh_admin_dict.get('id')} due to missing username.")
            return None

        placeholder_hashed_password = f"NEEDS_RESET_IN_MARZBAN__{secrets.token_hex(16)}"
        mzb_admin_data = {
            "username": username,
            "hashed_password": placeholder_hashed_password,
            "is_sudo": bool(mzsh_admin_dict.get("is_sudo", False)),
            "created_at": iso_format_to_datetime(mzsh_admin_dict.get("created_at")) or datetime.now(timezone.utc),
            "password_reset_at": iso_format_to_datetime(mzsh_admin_dict.get("password_reset_at")),
            "discord_webhook": mzsh_admin_dict.get("discord_webhook"), # Map if available, else None
            "users_usage": mzsh_admin_dict.get("users_usage", 0), # Default to 0 if not available
            "telegram_id": mzsh_admin_dict.get("telegram_id") # Map if available, else None
        }
        return mzb_admin_data

    # Fault-tolerant transformation methods
    
    @fault_tolerant("migration", "user_transformation")
    async def transform_user_with_fault_tolerance(self, mzsh_user_dict: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Transform user data with fault tolerance.
        
        Args:
            mzsh_user_dict: Marzneshin user data
            
        Returns:
            Tuple of (transformed user, list of proxies)
        """
        return self.transform_user_for_marzban(mzsh_user_dict)
    
    @fault_tolerant("migration", "admin_transformation")
    async def transform_admin_with_fault_tolerance(self, mzsh_admin_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform admin data with fault tolerance.
        
        Args:
            mzsh_admin_dict: Marzneshin admin data
            
        Returns:
            Transformed admin data
        """
        return self.transform_admin_for_marzban(mzsh_admin_dict)
    
    async def orchestrate_transformation_with_fault_tolerance(self, 
                                                            mzsh_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Orchestrate data transformation with comprehensive fault tolerance.
        
        Args:
            mzsh_data: Marzneshin data to transform
            
        Returns:
            Transformed Marzban data
        """
        logger.info("Starting fault-tolerant data transformation...")
        
        # Initialize result structure
        transformed_data = {"users": [], "proxies": [], "admins": []}
        
        # Transform users with fault tolerance
        user_retry_config = RetryConfig(max_attempts=3, base_delay=1.0)
        
        for user_data in mzsh_data.get("users", []):
            try:
                user_result = await self.fault_tolerance.execute_with_fault_tolerance(
                    self.transform_user_for_marzban,
                    "migration",
                    "user_transformation",
                    user_retry_config,
                    user_data
                )
                
                if user_result and user_result[0]:
                    transformed_user, user_proxies = user_result
                    transformed_data["users"].append(transformed_user)
                    transformed_data["proxies"].extend(user_proxies)
                    
            except Exception as e:
                username = user_data.get("username", "unknown")
                logger.error(f"Failed to transform user {username} with fault tolerance: {e}")
                continue
        
        # Transform admins with fault tolerance
        admin_retry_config = RetryConfig(max_attempts=2, base_delay=0.5)
        
        for admin_data in mzsh_data.get("admins", []):
            try:
                admin_result = await self.fault_tolerance.execute_with_fault_tolerance(
                    self.transform_admin_for_marzban,
                    "migration",
                    "admin_transformation", 
                    admin_retry_config,
                    admin_data
                )
                
                if admin_result:
                    transformed_data["admins"].append(admin_result)
                    
            except Exception as e:
                username = admin_data.get("username", "unknown")
                logger.error(f"Failed to transform admin {username} with fault tolerance: {e}")
                continue
        
        logger.info(f"Fault-tolerant transformation completed: "
                   f"{len(transformed_data['users'])} users, "
                   f"{len(transformed_data['proxies'])} proxies, "
                   f"{len(transformed_data['admins'])} admins")
        
        return transformed_data
    
    def get_fault_tolerance_status(self) -> Dict[str, Any]:
        """Get fault tolerance system status."""
        return self.fault_tolerance.get_system_health()
    
    def reset_fault_tolerance(self):
        """Reset all fault tolerance components."""
        self.fault_tolerance.reset_all_circuits()
        logger.info("Fault tolerance system reset")

    def orchestrate_transformation(self, mzsh_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(RichText("Transforming Marzneshin data to Marzban format...", style="bold yellow"))
        transformed_marzban_data: Dict[str, List[Dict[str, Any]]] = {"users": [], "proxies": [], "admins": []}
        users_without_any_proxy_info_count = 0
        users_using_fallback_key_as_proxy_count = 0
        processed_usernames_for_transformation = set()

        for mzsh_user_item in mzsh_data.get("users", []):
            username = mzsh_user_item.get("username")
            if not username:
                if logger.getEffectiveLevel() <= logging.DEBUG: logger.debug(f"Skipping source user data due to missing username: {mzsh_user_item.get('id')}")
                continue
            if username in processed_usernames_for_transformation:
                logger.warning(f"Duplicate username '{username}' encountered in source data during transformation. Skipping subsequent instance.")
                continue
            processed_usernames_for_transformation.add(username)

            mzb_user_transformed, mzb_proxies_transformed_list = self.transform_user_for_marzban(mzsh_user_item)
            if mzb_user_transformed:
                transformed_marzban_data["users"].append(mzb_user_transformed)
                if mzb_proxies_transformed_list:
                    transformed_marzban_data["proxies"].extend(mzb_proxies_transformed_list)
                    is_fallback_proxy = True
                    proxies_field_from_mzsh = mzsh_user_item.get("proxies", {})
                    if isinstance(proxies_field_from_mzsh, dict):
                        for _, detail_dict_val in proxies_field_from_mzsh.items():
                            if isinstance(detail_dict_val, dict) and detail_dict_val.get("id"):
                                is_fallback_proxy = False; break
                    if is_fallback_proxy and mzsh_user_item.get("key"):
                        users_using_fallback_key_as_proxy_count +=1
                else:
                    users_without_any_proxy_info_count +=1
        
        processed_admin_usernames_for_transformation = set()
        for mzsh_admin_item in mzsh_data.get("admins", []):
            admin_username = mzsh_admin_item.get("username")
            if not admin_username:
                if logger.getEffectiveLevel() <= logging.DEBUG: logger.debug(f"Skipping source admin data due to missing username: {mzsh_admin_item.get('id')}")
                continue
            if admin_username in processed_admin_usernames_for_transformation:
                logger.warning(f"Duplicate admin username '{admin_username}' encountered in source data during transformation. Skipping subsequent instance.")
                continue
            processed_admin_usernames_for_transformation.add(admin_username)

            mzb_admin_transformed = self.transform_admin_for_marzban(mzsh_admin_item)
            if mzb_admin_transformed:
                transformed_marzban_data["admins"].append(mzb_admin_transformed)

        if RICH_AVAILABLE:
            summary_table = Table(title=RichText("Transformation Summary",style="bold blue"), show_header=True, header_style="bold magenta", border_style="blue") # type: ignore
            summary_table.add_column("Entity", style="dim cyan", min_width=25) # type: ignore
            summary_table.add_column("Count Transformed", justify="right", style="green") # type: ignore
            summary_table.add_row("Admins", str(len(transformed_marzban_data['admins']))) # type: ignore
            summary_table.add_row("Users", str(len(transformed_marzban_data['users']))) # type: ignore
            summary_table.add_row("Proxy Entries (Total)", str(len(transformed_marzban_data['proxies']))) # type: ignore
            if users_using_fallback_key_as_proxy_count > 0:
                summary_table.add_row("Users using formatted 'key' as VLESS UUID", str(users_using_fallback_key_as_proxy_count)) # type: ignore
            if users_without_any_proxy_info_count > 0:
                summary_table.add_row("Users with NO proxy data generated", str(users_without_any_proxy_info_count)) # type: ignore
            console.print(summary_table)
        else:
            logger.info(f"Transformation complete. Marzban-formatted Admins: {len(transformed_marzban_data['admins'])}, Users: {len(transformed_marzban_data['users'])}, Proxy entries: {len(transformed_marzban_data['proxies'])}.")
            if users_using_fallback_key_as_proxy_count > 0: logger.info(f"... of which {users_using_fallback_key_as_proxy_count} users had their formatted 'key' used as a VLESS proxy UUID.")
            if users_without_any_proxy_info_count > 0: logger.warning(f"... and {users_without_any_proxy_info_count} users ended up with no proxy information to migrate.")

        logger.info(RichText("Data transformation phase completed.", style="bold green"))
        return transformed_marzban_data
