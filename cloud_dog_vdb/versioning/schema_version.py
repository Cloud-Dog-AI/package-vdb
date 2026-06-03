# Copyright 2026 Cloud-Dog, Viewdeck Engineering Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# cloud_dog_vdb — Schema version management
"""Track collection schema versions and migration requirements."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import warnings


@dataclass(frozen=True, slots=True)
class CollectionSchemaVersion:
    """Represent collection schema version."""

    collection: str
    version: int
    dimension_count: int
    metadata_fields: tuple[str, ...]
    embedding_model: str
    signature: str


@dataclass(frozen=True, slots=True)
class SchemaMigrationPlan:
    """Represent schema migration plan."""

    collection: str
    from_version: int
    to_version: int
    dimension_changed: bool
    metadata_changed: bool
    embedding_model_changed: bool
    reembed_required: bool
    warning: str


class SchemaVersionManager:
    """Manage collection schema signatures and migration decisions."""

    def __init__(self) -> None:
        self._versions: dict[str, CollectionSchemaVersion] = {}

    def register(
        self,
        collection: str,
        *,
        dimension_count: int,
        metadata_fields: list[str] | tuple[str, ...],
        embedding_model: str,
    ) -> CollectionSchemaVersion:
        """Handle register."""
        fields = tuple(sorted(str(field) for field in metadata_fields))
        existing = self._versions.get(collection)
        signature = self._signature(dimension_count, fields, embedding_model)
        version = 1 if existing is None else existing.version + (existing.signature != signature)
        record = CollectionSchemaVersion(
            collection=collection,
            version=version,
            dimension_count=dimension_count,
            metadata_fields=fields,
            embedding_model=embedding_model,
            signature=signature,
        )
        self._versions[collection] = record
        return record

    def current(self, collection: str) -> CollectionSchemaVersion | None:
        """Handle current."""
        return self._versions.get(collection)

    def warn_if_query_mismatch(
        self,
        collection: str,
        *,
        expected_dimension_count: int | None = None,
        expected_embedding_model: str | None = None,
    ) -> bool:
        """Handle warn if query mismatch."""
        current = self._versions.get(collection)
        if current is None:
            return False
        mismatches: list[str] = []
        if expected_dimension_count is not None and expected_dimension_count != current.dimension_count:
            mismatches.append(
                f"dimension mismatch: expected {expected_dimension_count}, actual {current.dimension_count}"
            )
        if expected_embedding_model and expected_embedding_model != current.embedding_model:
            mismatches.append(
                f"embedding model mismatch: expected {expected_embedding_model}, actual {current.embedding_model}"
            )
        if not mismatches:
            return False
        warnings.warn(
            f"Collection '{collection}' schema version mismatch: {', '.join(mismatches)}",
            RuntimeWarning,
            stacklevel=2,
        )
        return True

    def plan_migration(
        self,
        collection: str,
        *,
        dimension_count: int,
        metadata_fields: list[str] | tuple[str, ...],
        embedding_model: str,
    ) -> SchemaMigrationPlan:
        """Handle plan migration."""
        current = self._versions.get(collection)
        if current is None:
            raise ValueError(f"Collection not registered: {collection}")
        next_record = self.register(
            collection,
            dimension_count=dimension_count,
            metadata_fields=metadata_fields,
            embedding_model=embedding_model,
        )
        dimension_changed = current.dimension_count != next_record.dimension_count
        metadata_changed = current.metadata_fields != next_record.metadata_fields
        embedding_model_changed = current.embedding_model != next_record.embedding_model
        reembed_required = dimension_changed or embedding_model_changed
        warning = "Re-embedding required due to dimension/model change." if reembed_required else ""
        return SchemaMigrationPlan(
            collection=collection,
            from_version=current.version,
            to_version=next_record.version,
            dimension_changed=dimension_changed,
            metadata_changed=metadata_changed,
            embedding_model_changed=embedding_model_changed,
            reembed_required=reembed_required,
            warning=warning,
        )

    @staticmethod
    def _signature(dimension_count: int, metadata_fields: tuple[str, ...], embedding_model: str) -> str:
        payload = f"{dimension_count}|{','.join(metadata_fields)}|{embedding_model}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()
