import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from typing import Counter as CounterType
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, ValuesView

from arango import ArangoClient
from arango.database import StandardDatabase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from mypy_extensions import TypedDict
from pydantic import PositiveInt, validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV, make_data_platform_urn, make_dataset_urn_with_platform_instance, make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    Schemaless,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    StatusClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)

# These are ArangoDB-internal databases, which we want to skip.
DENY_DATABASE_LIST = set(["_system"])


class ArangoDBConfig(ConfigModel):
    # See the ArangoDB authentication docs for details and examples.
    host_port: str = "http://localhost:8529"
    username: Optional[str] = None
    password: Optional[str] = None
    database: str
    schemaSamplingSize: Optional[PositiveInt] = 1000
    maxSchemaSize: Optional[PositiveInt] = 300
    # mongodb only supports 16MB as max size for documents. However, if we try to retrieve a larger document it
    # errors out with "16793600" as the maximum size supported.
    maxDocumentSize: Optional[PositiveInt] = 16793600
    env: str = DEFAULT_ENV
    options: dict = {}

    collection_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    @validator("maxDocumentSize")
    def check_max_doc_size_filter_is_valid(cls, doc_size_filter_value):
        if doc_size_filter_value > 16793600:
            raise ValueError("maxDocumentSize must be a positive value <= 16793600.")
        return doc_size_filter_value


@dataclass
class ArangoDBSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# map PyArango types to canonical MongoDB strings
PYARANGO_TYPE_TO_ARANGO_TYPE = {
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "null",
    bool: "boolean",
    int: "integer",
    float: "float",
    str: "string",
    "mixed": "mixed",
}


# map PyArango types to DataHub classes
_field_type_mapping: Dict[Union[Type, str], Type] = {
    list: ArrayTypeClass,
    bool: BooleanTypeClass,
    type(None): NullTypeClass,
    int: NumberTypeClass,
    float: NumberTypeClass,
    str: StringTypeClass,
    dict: RecordTypeClass,
    "mixed": UnionTypeClass,
}


def is_nullable_doc(doc: Dict[str, Any], field_path: Tuple) -> bool:
    """
    Check if a nested field is nullable in a document from a collection.

    Parameters
    ----------
        doc:
            document to check nullability for
        field_path:
            path to nested field to check, ex. ('first_field', 'nested_child', '2nd_nested_child')
    """

    field = field_path[0]

    # if field is inside
    if field in doc:

        value = doc[field]

        if value is None:
            return True

        # if no fields left, must be non-nullable
        if len(field_path) == 1:
            return False

        # otherwise, keep checking the nested fields
        remaining_fields = field_path[1:]

        # if dictionary, check additional level of nesting
        if isinstance(value, dict):
            return is_nullable_doc(doc[field], remaining_fields)

        # if list, check if any member is missing field
        if isinstance(value, list):

            # count empty lists of nested objects as nullable
            if len(value) == 0:
                return True

            return any(is_nullable_doc(x, remaining_fields) for x in doc[field])

        # any other types to check?
        # raise ValueError("Nested type not 'list' or 'dict' encountered")
        return True

    return True


def is_nullable_collection(
    collection: Iterable[Dict[str, Any]], field_path: Tuple
) -> bool:
    """
    Check if a nested field is nullable in a collection.

    Parameters
    ----------
        collection:
            collection to check nullability for
        field_path:
            path to nested field to check, ex. ('first_field', 'nested_child', '2nd_nested_child')
    """

    return any(is_nullable_doc(doc, field_path) for doc in collection)


class BasicSchemaDescription(TypedDict):
    types: CounterType[type]  # field types and times seen
    count: int  # times the field was seen


class SchemaDescription(BasicSchemaDescription):
    delimited_name: str  # collapsed field name
    # we use 'mixed' to denote mixed types, so we need a str here
    type: Union[type, str]  # collapsed type
    nullable: bool  # if field is ever missing


def construct_schema(
    collection: Iterable[Dict[str, Any]], delimiter: str
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Construct (infer) a schema from a collection of documents.

    For each field (represented as a tuple to handle nested items), reports the following:
        - `types`: Python types of field values
        - `count`: Number of times the field was encountered
        - `type`: type of the field if `types` is just a single value, otherwise `mixed`
        - `nullable`: if field is ever null/missing
        - `delimited_name`: name of the field, joined by a given delimiter

    Parameters
    ----------
        collection:
            collection to construct schema over.
        delimiter:
            string to concatenate field names by
    """

    schema: Dict[Tuple[str, ...], BasicSchemaDescription] = {}

    def append_to_schema(doc: Dict[str, Any], parent_prefix: Tuple[str, ...]) -> None:
        """
        Recursively update the schema with a document, which may/may not contain nested fields.

        Parameters
        ----------
            doc:
                document to scan
            parent_prefix:
                prefix of fields that the document is under, pass an empty tuple when initializing
        """

        for key, value in doc.items():

            new_parent_prefix = parent_prefix + (key,)

            # if nested value, look at the types within
            if isinstance(value, dict):

                append_to_schema(value, new_parent_prefix)

            # if array of values, check what types are within
            if isinstance(value, list):

                for item in value:

                    # if dictionary, add it as a nested object
                    if isinstance(item, dict):
                        append_to_schema(item, new_parent_prefix)

            # don't record None values (counted towards nullable)
            if value is not None:

                if new_parent_prefix not in schema:

                    schema[new_parent_prefix] = {
                        "types": Counter([type(value)]),
                        "count": 1,
                    }

                else:

                    # update the type count
                    schema[new_parent_prefix]["types"].update({type(value): 1})
                    schema[new_parent_prefix]["count"] += 1

    for document in collection:
        append_to_schema(document, ())

    extended_schema: Dict[Tuple[str, ...], SchemaDescription] = {}

    for field_path in schema.keys():

        field_types = schema[field_path]["types"]

        field_type: Union[str, type] = "mixed"

        # if single type detected, mark that as the type to go with
        if len(field_types.keys()) == 1:
            field_type = next(iter(field_types))

        field_extended: SchemaDescription = {
            "types": schema[field_path]["types"],
            "count": schema[field_path]["count"],
            "nullable": is_nullable_collection(collection, field_path),
            "delimited_name": delimiter.join(field_path),
            "type": field_type,
        }

        extended_schema[field_path] = field_extended

    return extended_schema


def construct_schema_pyarango(
    collection: StandardDatabase,
    delimiter: str,
    sample_size: Optional[int] = None,
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Calls construct_schema on a PyArango collection.

    Returned schema is keyed by tuples of nested field names, with each
    value containing 'types', 'count', 'nullable', 'delimited_name', and 'type' attributes.
    """
    if sample_size:
        documentsCursor = collection.export(sample_size, ttl=200)
    else:
        documentsCursor = collection.export(ttl=200)
    documents_to_list = [doc for doc in documentsCursor]

    return construct_schema(list(documents_to_list), delimiter)


@dataclass
class ArangoDBSource(Source):
    config: ArangoDBConfig
    report: ArangoDBSourceReport
    arango_client: ArangoClient
    db: StandardDatabase

    def __init__(self, ctx: PipelineContext, config: ArangoDBConfig):
        super().__init__(ctx)
        self.config = config
        self.report = ArangoDBSourceReport()

        options = {}
        if self.config.username is not None:
            options["username"] = self.config.username
        if self.config.password is not None:
            options["password"] = self.config.password
        if self.config.database is not None:
            options['name'] = self.config.database

        options = {
            **options,
            **self.config.options,
        }
        self.arango_client = ArangoClient(hosts=self.config.host_port)
        self.db = self.arango_client.db(**options)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ArangoDBSourceReport":
        config = ArangoDBConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_pyarango_type_string(
        self, field_type: Union[Type, str], collection_name: str
    ) -> str:
        """
        Return Mongo type string from a Python type

        Parameters
        ----------
            field_type:
                type of a Python object
            collection_name:
                name of collection (for logging)
        """
        try:
            type_string = PYARANGO_TYPE_TO_ARANGO_TYPE[field_type]
        except KeyError:
            self.report.report_warning(
                collection_name, f"unable to map type {field_type} to metadata schema"
            )
            PYARANGO_TYPE_TO_ARANGO_TYPE[field_type] = "unknown"
            type_string = "unknown"

        return type_string

    def get_field_type(
        self, field_type: Union[Type, str], collection_name: str
    ) -> SchemaFieldDataType:
        """
        Maps types encountered in PyArango to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of a Python object
            collection_name:
                name of collection (for logging)
        """
        TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.report_warning(
                collection_name, f"unable to map type {field_type} to metadata schema"
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        platform = "arangodb"

        # traverse databases in sorted order so output is consistent
        database_name = self.config.database
        database = self.db
        collections: List[str] = database.collections()

        # traverse collections in sorted order so output is consistent
        for collection in sorted(collections, key=lambda col: col['name']):
            isSystem = collection['system']
            collection_name = collection['name']
            collection_type = collection['type']
            dataset_name = f"{database_name}.{collection_name}"

            if not self.config.collection_pattern.allowed(dataset_name) or isSystem:
                self.report.report_dropped(dataset_name)
                continue

            dataset_urn = make_dataset_urn_with_platform_instance(
                platform=platform,
                name=dataset_name,
                env=self.config.env,
                platform_instance=""
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[],
            )

            dataset_properties = DatasetPropertiesClass(
                tags=[],
                customProperties={},
            )
            dataset_snapshot.aspects.append(dataset_properties)

            assert self.config.maxDocumentSize is not None
            collection_schema = construct_schema_pyarango(
                database[collection_name],
                delimiter=".",
                sample_size=self.config.schemaSamplingSize,
            )

            # initialize the schema for the collection
            canonical_schema: List[SchemaField] = []
            max_schema_size = self.config.maxSchemaSize
            collection_schema_size = len(collection_schema.values())
            collection_fields: Union[
                List[SchemaDescription], ValuesView[SchemaDescription]
            ] = collection_schema.values()
            assert max_schema_size is not None
            if collection_schema_size > max_schema_size:
                # downsample the schema, using frequency as the sort key
                self.report.report_warning(
                    key=dataset_urn,
                    reason=f"Downsampling the collection schema because it has {collection_schema_size} fields. Threshold is {max_schema_size}",
                )
                collection_fields = sorted(
                    collection_schema.values(),
                    key=lambda x: x["count"],
                    reverse=True,
                )[0:max_schema_size]
                # Add this information to the custom properties so user can know they are looking at downsampled schema
                dataset_properties.customProperties[
                    "schema.downsampled"
                ] = "True"
                dataset_properties.customProperties[
                    "schema.totalFields"
                ] = f"{collection_schema_size}"

            logger.debug(
                f"Size of collection fields = {len(collection_fields)}"
            )
            # append each schema field (sort so output is consistent)
            for schema_field in sorted(
                collection_fields, key=lambda x: x["delimited_name"]
            ):
                field = SchemaField(
                    fieldPath=schema_field["delimited_name"],
                    nativeDataType=self.get_pyarango_type_string(
                        schema_field["type"], dataset_name
                    ),
                    type=self.get_field_type(
                        schema_field["type"], dataset_name
                    ),
                    description=None,
                    nullable=schema_field["nullable"],
                    recursive=False,
                )
                canonical_schema.append(field)

            # create schema metadata object for collection
            schema_metadata = SchemaMetadata(
                schemaName=collection_name,
                platform=make_data_platform_urn(platform),
                version=0,
                hash="",
                platformSchema=Schemaless(),
                fields=canonical_schema,
            )

            mcp_schema = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="schemaMetadata",
                aspect=schema_metadata,
                changeType=ChangeTypeClass.UPSERT,
            )

            mcp_tag = MetadataChangeProposalWrapper(
                entityType='dataset',
                entityUrn=dataset_urn,
                aspectName='globalTags',
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=make_tag_urn(collection_type))]),
                changeType=ChangeTypeClass.UPSERT
            )
            for mcp in [mcp_schema, mcp_tag]:
                wu = MetadataWorkUnit(id=dataset_name, mcp=mcp)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> ArangoDBSourceReport:
        return self.report

    def close(self):
        self.arango_client.close()
