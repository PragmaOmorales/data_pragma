"""Operators for Jira Service Management Ops integration with Airflow.


"""


import re
from typing import Any, Dict, List, Optional, Sequence
from datetime import timedelta
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator


class JiraCreateAlert(BaseOperator):
    """
    Envía una alerta a Jira Service Management Ops.

    """

    template_fields: Sequence[str] = (
        'message', 'alias', 'description', 'entity', 'source', 'priority', 'note', 'details'
    )

    API_PATH = "/jsm/ops/integration/v2/alerts"

    retry_exponential_backoff = True
    retries = 3
    retry_delay = timedelta(seconds=30)
    max_retry_delay = timedelta(minutes=10)

    def __init__(
        self,
        *,
        message: str,
        opsgenie_conn_id: str = 'jira_ops_default',
        alias: Optional[str] = None,
        description: Optional[str] = None,
        responders: Optional[List[Dict[str, Any]]] = None,
        visible_to: Optional[List[Dict[str, Any]]] = None,
        actions: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
        entity: Optional[str] = None,
        source: str = 'Airflow',
        priority: Optional[str] = 'P5',
        user: Optional[str] = None,
        note: Optional[str] = None,
        **kwargs
    ):
        """Initialize the create alert operator."""
        super().__init__(**kwargs)
        self.message = message
        self.opsgenie_conn_id = opsgenie_conn_id
        self.alias = alias
        self.description = description
        self.responders = responders or []
        self.visible_to = visible_to or []
        self.actions = actions or []
        self.tags = tags or []
        self.details = details or {}
        self.entity = entity
        self.source = source
        self.priority = priority
        self.user = user
        self.note = note

    def _validate(self):
        """Validate required parameters."""
        if not self.message:
            raise AirflowException("Especifica el mensaje; es el corazón de la alerta.")

    def _ensure_alias(self) -> str:
        """Generate alias if not provided."""
        if self.alias:
            return self.alias
        return re.sub(r'\W+', '_', self.message).strip('_')[:50]

    def _build_payload(self) -> Dict[str, Any]:
        """Build the payload for the Opsgenie API request."""
        payload: Dict[str, Any] = {
            "message":    self.message,
            "alias":      self._ensure_alias(),
            "description": self.description or "",
            "responders": self.responders,
            "visibleTo":  self.visible_to,
            "actions":    self.actions,
            "tags":       self.tags,
            "details":    self.details,
            "entity":     self.entity,
            "source":     self.source,
            "priority":   self.priority,
            "user":       self.user,
            "note":       self.note,
        }
        return {k: v for k, v in payload.items() if v not in (None, [], {})}

    def execute(self, context) -> Dict[str, Any]:
        """Execute the operator's main logic."""
        self._validate()
        conn = BaseHook.get_connection(self.opsgenie_conn_id)
        apikey = conn.password
        if not apikey:
            raise AirflowException(
                f"Falta tu API key en la conexión '{self.opsgenie_conn_id}'. Revísalo."
            )
        headers = {
            "Accept":        "application/json",
            "Content-Type":  "application/json",
            "Authorization": f"GenieKey {apikey}",
        }
        hook = HttpHook(http_conn_id=self.opsgenie_conn_id, method="POST")
        self.log.info("Enviando alerta a Jira Ops (%s)...", self.API_PATH)
        response = hook.run(
            endpoint=self.API_PATH,
            json=self._build_payload(),
            headers=headers,
        )

        self.log.info("Alerta enviada con éxito! requestId=%s", response.json().get('requestId'))
        return response.json()