# migration/marzneshin_client/client.py

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx
from pydantic import BaseModel

from core.config import AppConfig
from core.exceptions import APIRequestError
from core.logging_setup import get_logger # Import the centralized logger
from utils.retry_decorator import async_retry_operation

# Import Marzneshin models
from .models import (
    MarzneshinLoginToken, MarzneshinUserList, MarzneshinAdminList,
    MarzneshinServiceList, MarzneshinInboundList
)

# Use the centralized logger
logger = get_logger()

class MarzneshinAPIClient:
    """Asynchronous client for interacting with the Marzneshin API."""
    def __init__(self, config: AppConfig):
        self.base_url = str(config.MARZNESHIN_API_URL).rstrip("/")
        self.username = config.MARZNESHIN_USERNAME
        self.password_secret = config.MARZNESHIN_PASSWORD
        self.config = config
        self._token: Optional[str] = None
        self._is_sudo_token: Optional[bool] = None
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            http2=True,
            follow_redirects=True
        )

    async def __aenter__(self) -> 'MarzneshinAPIClient':
        await self._client.__aenter__()
        await self._ensure_login()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._client.__aexit__(exc_type, exc_val, exc_tb)

    async def _ensure_login(self) -> None:
        if not self._token:
            await self.login()

    @async_retry_operation # type: ignore
    async def login(self) -> None:
        logger.info(f"Attempting Marzneshin login: [cyan]{self.base_url}[/] as [yellow]'{self.username}'[/]")
        try:
            response = await self._client.post(
                f"{self.base_url}/api/admins/token",
                data={"username": self.username, "password": self.password_secret.get_secret_value()},
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            response.raise_for_status()
            token_data = MarzneshinLoginToken(**response.json())
            self._token = token_data.access_token
            self._is_sudo_token = token_data.is_sudo
            logger.info(f"Marzneshin login [bold green]successful[/]. Sudo access: [bold magenta]{self._is_sudo_token}[/]")
            if self._is_sudo_token is False:
                logger.warning("Logged-in Marzneshin user lacks sudo privileges. Some data might be inaccessible.")
        except httpx.HTTPStatusError as e:
            raise APIRequestError(f"Login HTTP error: {e.response.status_code} - {e.response.text[:150]}", e.response.status_code, e.response.text) from e
        except Exception as e:
            raise APIRequestError(f"Login unexpected error: {type(e).__name__} - {str(e)[:150]}") from e

    @async_retry_operation # type: ignore
    async def _api_call(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await self._ensure_login()
        if not self._token:
            raise APIRequestError("Marzneshin authentication token is unavailable after login attempt.")

        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if json_body: headers["Content-Type"] = "application/json"

        url = f"{self.base_url}{endpoint}"
        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(f"Mzsh Call: {method} {url} | Params: {params} | JSON body present: {json_body is not None}")

        try:
            response = await self._client.request(method, url, headers=headers, params=params, json=json_body)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._token = None
                logger.warning(f"Marzneshin API returned 401 for {url}. Token cleared for re-authentication.")
            raise APIRequestError(f"API call to {url} failed: {e.response.status_code} - {e.response.text[:150]}", e.response.status_code, e.response.text) from e
        except httpx.RequestError as e:
            raise APIRequestError(f"Network error for API call to {url}: {e}") from e
        except json.JSONDecodeError as e:
            response_text = getattr(response, 'text', 'N/A')
            raise APIRequestError(f"Failed to decode JSON response from {url}: {e}. Response text: {response_text[:150]}") from e

    async def _fetch_all_pages(self, endpoint: str, model_type: Type[BaseModel], entity_name: str, progress: Optional[Any] = None, task_id: Optional[Any] = None) -> List[Dict[str, Any]]:
        # progress and task_id are Any to avoid direct Rich dependency here, handled by caller
        all_items: List[Dict[str, Any]] = []
        offset = 0
        limit = self.config.API_BATCH_SIZE
        initial_api_total_reported: Optional[int] = None

        if progress and task_id is not None and hasattr(progress, "update"):
            progress.update(task_id, description=f"Fetching {entity_name} (Batch: {limit}, Delay: {self.config.API_REQUEST_DELAY_S}s)", total=None, advance=0)
        else:
            logger.info(f"Fetching all {entity_name} from Marzneshin at {endpoint} (batch size: {limit}, API delay: {self.config.API_REQUEST_DELAY_S}s)...")

        page_count = 0
        while True:
            page_count += 1
            if logger.getEffectiveLevel() <= logging.DEBUG:
                 logger.debug(f"Fetching {entity_name}, page {page_count}, offset {offset}, limit {limit}...")
            try:
                page_json_data = await self._api_call("GET", endpoint, params={"offset": offset, "limit": limit})
                page_data_validated = model_type(**page_json_data)
                items_on_this_page = getattr(page_data_validated, "items", [])

                current_page_api_total_debug: Optional[int] = getattr(page_data_validated, "total", None)
                if isinstance(current_page_api_total_debug, int) and page_count == 1 and current_page_api_total_debug > 0:
                    initial_api_total_reported = current_page_api_total_debug
                    if progress and task_id is not None and hasattr(progress, "update"):
                        progress.update(task_id, total=initial_api_total_reported)

                if logger.getEffectiveLevel() <= logging.DEBUG:
                    logger.debug(f"API page {page_count} for {entity_name} reported total: {current_page_api_total_debug}. Initial API total from page 1: {initial_api_total_reported}")

                if not items_on_this_page:
                    logger.info(f"API returned no more {entity_name} items at offset {offset} (page {page_count}). Finalizing fetch for {entity_name}.")
                    break

                current_batch_as_dicts = [item.model_dump(mode='json', by_alias=True) for item in items_on_this_page]
                all_items.extend(current_batch_as_dicts)

                progress_bar_display_total_on_update = initial_api_total_reported
                if initial_api_total_reported is not None and len(all_items) > initial_api_total_reported and items_on_this_page:
                    progress_bar_display_total_on_update = None

                progress_desc = f"Fetching {entity_name}: {len(all_items)}"
                if progress_bar_display_total_on_update is not None:
                    progress_desc += f"/{progress_bar_display_total_on_update}"
                else:
                    progress_desc += "/?"

                if progress and task_id is not None and hasattr(progress, "update"):
                    progress.update(task_id, advance=len(current_batch_as_dicts), description=progress_desc, total=progress_bar_display_total_on_update)
                else:
                    logger.info(f"Fetched {len(current_batch_as_dicts)} {entity_name} this page (total gathered {len(all_items)}). Initial API total: {initial_api_total_reported if initial_api_total_reported is not None else 'N/A'}")

                if initial_api_total_reported is not None and initial_api_total_reported > 0 and len(all_items) >= initial_api_total_reported:
                    logger.info(f"All {initial_api_total_reported} {entity_name} fetched according to server total. Concluding fetch.")
                    break
                if len(items_on_this_page) < limit:
                    logger.info(f"API returned {len(items_on_this_page)} {entity_name} (less than limit {limit}). Assuming this is the last page for {entity_name}.")
                    break

                offset += len(current_batch_as_dicts)
                await asyncio.sleep(self.config.API_REQUEST_DELAY_S)

            except APIRequestError as e:
                logger.error(f"API error during paginated fetch for {entity_name} (offset {offset}): {e}. Halting {entity_name} fetch.")
                break
            except ValidationError as e_val: # Pydantic validation error
                logger.error(f"Response validation error for {entity_name} page (offset {offset}): {e_val}. Halting {entity_name} fetch.")
                break
            except Exception as e:
                logger.error(f"Unexpected error while processing {entity_name} page (offset {offset}): {type(e).__name__} - {e}. Halting.", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
                break

        if progress and task_id is not None and hasattr(progress, "update"):
            final_total_fetched = len(all_items)
            final_description = f"[bold green]Finished fetching {entity_name}: {final_total_fetched} total.[/]"
            if initial_api_total_reported is not None and final_total_fetched != initial_api_total_reported :
                 final_description += f" (API initially reported {initial_api_total_reported})"
            progress.update(task_id, completed=final_total_fetched, total=final_total_fetched if final_total_fetched > 0 else None, description=final_description, visible=True)
        else:
            log_msg_final = f"Finished paginated fetching for {entity_name}. Total items extracted: {len(all_items)}."
            if initial_api_total_reported is not None and len(all_items) != initial_api_total_reported :
                log_msg_final += f" (Initial API reported total: {initial_api_total_reported})"
            elif initial_api_total_reported is None and len(all_items) > 0:
                log_msg_final += " (API did not provide initial total or reported 0)"
            logger.info(log_msg_final)

        return all_items

    async def get_all_users_with_proxies(self, progress: Optional[Any] = None, task_id: Optional[Any] = None) -> List[Dict[str, Any]]:
        return await self._fetch_all_pages("/api/users", MarzneshinUserList, "users", progress, task_id)

    async def get_all_admins(self, progress: Optional[Any] = None, task_id: Optional[Any] = None) -> List[Dict[str, Any]]:
        return await self._fetch_all_pages("/api/admins", MarzneshinAdminList, "admins", progress, task_id)

    async def get_all_services(self, progress: Optional[Any] = None, task_id: Optional[Any] = None) -> List[Dict[str, Any]]:
        return await self._fetch_all_pages("/api/services", MarzneshinServiceList, "services", progress, task_id)

    async def get_all_inbounds(self, progress: Optional[Any] = None, task_id: Optional[Any] = None) -> List[Dict[str, Any]]:
        return await self._fetch_all_pages("/api/inbounds", MarzneshinInboundList, "inbounds", progress, task_id)
