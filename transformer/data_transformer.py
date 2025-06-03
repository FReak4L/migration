# migration/transformer/data_transformer.py

import json
import logging
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.config import AppConfig
from core.logging_setup import get_logger, get_console, RICH_AVAILABLE, RichText # type: ignore
from core.schema_analyzer import SchemaAnalyzer
from core.schema_compatibility import SchemaCompatibilityEngine, ConversionStrategy
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
    """Enhanced data transformer with schema compatibility engine."""
    def __init__(self, config: AppConfig):
        self.config = config
        self.schema_engine = SchemaCompatibilityEngine(ConversionStrategy.LENIENT)
        self.transformation_stats = {
            "users_processed": 0,
            "users_transformed": 0,
            "admins_processed": 0,
            "admins_transformed": 0,
            "conversion_errors": 0,
            "fallback_conversions": 0
        }

    def _convert_key_to_uuid_format(self, key_str: str) -> Optional[str]:
        """Converts a 32-character hex string to standard UUID format with dashes."""
        # Use the enhanced schema compatibility engine for UUID conversion
        result = self.schema_engine._convert_uuid_format(key_str, add_dashes=True)
        if result is None:
            self.transformation_stats["conversion_errors"] += 1
        return result
    
    def transform_user_with_schema_engine(self, mzsh_user_dict: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """Enhanced user transformation using schema compatibility engine."""
        username = mzsh_user_dict.get("username")
        if not username:
            logger.debug(f"Skipping Marzneshin user with ID {mzsh_user_dict.get('id')} due to missing username.")
            return None, []
        
        self.transformation_stats["users_processed"] += 1
        
        # Use schema compatibility engine for basic field transformations
        mappings = self.schema_engine.get_marzneshin_to_marzban_user_mappings()
        base_user_data = self.schema_engine.transform_record(mzsh_user_dict, mappings)
        
        # Add custom logic for status determination and other complex transformations
        base_user_data["status"] = self._determine_mzb_user_status(mzsh_user_dict)
        base_user_data["admin_id"] = None
        base_user_data["on_hold_timeout"] = None
        base_user_data["on_hold_expire_duration"] = None
        base_user_data["auto_delete_in_days"] = None
        base_user_data["last_status_change"] = datetime.now(timezone.utc)
        
        # Handle on_hold status special case
        if base_user_data["status"] == "on_hold":
            base_user_data["on_hold_timeout"] = iso_format_to_datetime(mzsh_user_dict.get("activation_deadline"))
            base_user_data["on_hold_expire_duration"] = mzsh_user_dict.get("usage_duration")
            base_user_data["expire"] = None
        
        # Ensure edit_at is not before created_at
        if base_user_data.get("edit_at") and base_user_data.get("created_at"):
            if base_user_data["edit_at"] < base_user_data["created_at"]:
                base_user_data["edit_at"] = base_user_data["created_at"]
        
        # Handle proxy transformations
        mzb_proxies_list = self._transform_user_proxies(mzsh_user_dict, username)
        
        self.transformation_stats["users_transformed"] += 1
        return base_user_data, mzb_proxies_list
    
    def _transform_user_proxies(self, mzsh_user_dict: Dict[str, Any], username: str) -> List[Dict[str, Any]]:
        """Transform user proxy configurations."""
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
                self.transformation_stats["fallback_conversions"] += 1
            else:
                logger.warning(f"User '{username}' fallback key '{user_key_as_main_uuid}' could not be formatted to UUID. Proxy entry from key will be skipped.")
                self.transformation_stats["conversion_errors"] += 1

        return mzb_proxies_list

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

        # Enhanced expire field conversion with proper error handling
        expire_value = None
        expire_date_raw = mzsh_user_dict.get("expire_date")
        if expire_date_raw:
            try:
                expire_dt = iso_format_to_datetime(expire_date_raw)
                expire_value = datetime_to_unix_timestamp(expire_dt)
                logger.debug(f"Converted expire_date '{expire_date_raw}' to Unix timestamp: {expire_value}")
            except Exception as e:
                logger.warning(f"Failed to convert expire_date '{expire_date_raw}' for user '{username}': {e}")
                expire_value = None

        mzb_user_data: Dict[str, Any] = {
            "username": username,
            "status": self._determine_mzb_user_status(mzsh_user_dict),
            "data_limit": mzsh_user_dict.get("data_limit", 0) or 0,
            "used_traffic": mzsh_user_dict.get("used_traffic", 0) or 0,
            "expire": expire_value,
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

    def analyze_schema_compatibility(self, mzsh_data: Dict[str, List[Dict[str, Any]]]) -> None:
        """Analyze schema compatibility and log findings."""
        logger.info(RichText("Analyzing schema compatibility...", style="bold cyan"))
        
        users_data = mzsh_data.get("users", [])
        admins_data = mzsh_data.get("admins", [])
        
        if users_data:
            user_analysis = self.schema_analyzer.analyze_user_schema_compatibility(users_data)
            logger.info(f"User schema compatibility: {user_analysis.compatibility_score:.2%}")
            
            if user_analysis.incompatibilities:
                logger.warning(f"Found {len(user_analysis.incompatibilities)} user schema incompatibilities:")
                for incomp in user_analysis.incompatibilities:
                    logger.warning(f"  - {incomp.field_name}: {incomp.source_type.value} -> {incomp.target_type.value} ({incomp.severity})")
                    if incomp.conversion_function:
                        logger.info(f"    Auto-conversion available: {incomp.conversion_function}")
        
        if admins_data:
            admin_analysis = self.schema_analyzer.analyze_admin_schema_compatibility(admins_data)
            logger.info(f"Admin schema compatibility: {admin_analysis.compatibility_score:.2%}")
            
            if admin_analysis.incompatibilities:
                logger.warning(f"Found {len(admin_analysis.incompatibilities)} admin schema incompatibilities:")
                for incomp in admin_analysis.incompatibilities:
                    logger.warning(f"  - {incomp.field_name}: {incomp.source_type.value} -> {incomp.target_type.value} ({incomp.severity})")

    def orchestrate_transformation(self, mzsh_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        # Perform schema analysis first
        self.analyze_schema_compatibility(mzsh_data)
        
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
    
    def get_transformation_statistics(self) -> Dict[str, Any]:
        """Get detailed transformation statistics."""
        schema_report = self.schema_engine.get_conversion_report()
        
        return {
            "transformation_stats": self.transformation_stats,
            "schema_conversion_report": schema_report,
            "success_rate": {
                "users": (self.transformation_stats["users_transformed"] / 
                         max(self.transformation_stats["users_processed"], 1)) * 100,
                "admins": (self.transformation_stats["admins_transformed"] / 
                          max(self.transformation_stats["admins_processed"], 1)) * 100
            }
        }
    
    def orchestrate_enhanced_transformation(self, mzsh_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """Enhanced transformation orchestration with improved error handling and statistics."""
        logger.info(RichText("Starting enhanced transformation with schema compatibility engine...", style="bold yellow"))
        
        # Clear previous statistics
        self.transformation_stats = {
            "users_processed": 0,
            "users_transformed": 0,
            "admins_processed": 0,
            "admins_transformed": 0,
            "conversion_errors": 0,
            "fallback_conversions": 0
        }
        self.schema_engine.clear_conversion_log()
        
        transformed_marzban_data: Dict[str, List[Dict[str, Any]]] = {"users": [], "proxies": [], "admins": []}
        processed_usernames = set()
        processed_admin_usernames = set()
        
        # Transform users with enhanced schema engine
        for mzsh_user_item in mzsh_data.get("users", []):
            username = mzsh_user_item.get("username")
            if not username:
                logger.debug(f"Skipping source user data due to missing username: {mzsh_user_item.get('id')}")
                continue
                
            if username in processed_usernames:
                logger.warning(f"Duplicate username '{username}' encountered. Skipping subsequent instance.")
                continue
                
            processed_usernames.add(username)
            
            try:
                mzb_user_transformed, mzb_proxies_transformed_list = self.transform_user_with_schema_engine(mzsh_user_item)
                if mzb_user_transformed:
                    transformed_marzban_data["users"].append(mzb_user_transformed)
                    if mzb_proxies_transformed_list:
                        transformed_marzban_data["proxies"].extend(mzb_proxies_transformed_list)
            except Exception as e:
                logger.error(f"Failed to transform user '{username}': {e}")
                self.transformation_stats["conversion_errors"] += 1
        
        # Transform admins
        for mzsh_admin_item in mzsh_data.get("admins", []):
            admin_username = mzsh_admin_item.get("username")
            if not admin_username:
                logger.debug(f"Skipping source admin data due to missing username: {mzsh_admin_item.get('id')}")
                continue
                
            if admin_username in processed_admin_usernames:
                logger.warning(f"Duplicate admin username '{admin_username}' encountered. Skipping subsequent instance.")
                continue
                
            processed_admin_usernames.add(admin_username)
            self.transformation_stats["admins_processed"] += 1
            
            try:
                mzb_admin_transformed = self.transform_admin_for_marzban(mzsh_admin_item)
                if mzb_admin_transformed:
                    transformed_marzban_data["admins"].append(mzb_admin_transformed)
                    self.transformation_stats["admins_transformed"] += 1
            except Exception as e:
                logger.error(f"Failed to transform admin '{admin_username}': {e}")
                self.transformation_stats["conversion_errors"] += 1
        
        # Log transformation statistics
        stats = self.get_transformation_statistics()
        logger.info(f"Transformation completed: {stats['transformation_stats']}")
        
        if stats['schema_conversion_report']['total_errors'] > 0:
            logger.warning(f"Schema conversion errors: {stats['schema_conversion_report']['total_errors']}")
        
        return transformed_marzban_data
