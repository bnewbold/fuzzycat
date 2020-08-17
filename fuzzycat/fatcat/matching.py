# coding: utf-8
"""
Public API for fuzzy matches for fatcat.

Match methods return candidates, verify methods return a match status.

    match_containar_fuzzy  -> List[ContainerEntity]
    match_release_fuzzy    -> List[ReleaseEntity]

    verify_serial_name     -> MatchStatus
    verify_container_name  -> MatchStatus
    verify_container_fuzzy -> MatchStatus
    verify_release_fuzzy   -> MatchStatus

Candidate generation will use external data from search and hence is expensive. Verification is fast.
"""

from typing import List

import elasticsearch
from fatcat_openapi_client import (ApiException, ContainerEntity, DefaultApi, ReleaseEntity,
                                   ReleaseExtIds, WorkEntity)
from fatcat_openapi_client.api.default_api import DefaultApi

from fuzzycat.fatcat.common import MatchStatus, response_to_entity_list
from fuzzycat.serials import serialsdb


def match_container_fuzzy(container: ContainerEntity,
                          size: int = 5,
                          es: Optional[Union[str, elasticsearch.client.Elasticsearch]] = None,
                          api: Optional[DefaultApi] = None) -> List[ContainerEntity]:
    """
    Given a container entity, which can be (very) partial, return a list of
    candidate matches. Elasticsearch can be a hostport or the low level client
    object.

    Random data point: with 20 parallel workers callind match_container_fuzzy,
    we get around 40 req/s.
    """
    assert isinstance(container, ContainerEntity)

    if size is None or size == 0:
        size = 10000  # or any large number

    if isinstance(es, str):
        es = elasticsearch.Elasticsearch([es])
    if es is None:
        es = elasticsearch.Elasticsearch()

    # If we find any match by ISSN-L, we return only those.
    if container.issnl:
        s = (elasticsearch_dsl.Search(using=es, index="fatcat_container").query(
            "term", issns=container.issnl).extra(size=size))
        resp = s.execute()
        if len(resp) > 0:
            return response_to_entity_list(resp, entity_type=ContainerEntity, api=api)

    # Do we have an exact QID match?
    if container.wikidata_qid:
        s = (elasticsearch_dsl.Search(using=es, index="fatcat_container").query(
            "term", wikidata_qid=container.wikidata_qid).extra(size=size))
        resp = s.execute()
        if len(resp) > 0:
            return response_to_entity_list(resp, entity_type=ContainerEntity, api=api)

    # Start with exact name match.
    #
    # curl -s https://search.fatcat.wiki/fatcat_container/_mapping  | jq .
    #
    # "name": {
    #   "type": "text",
    #   "copy_to": [
    #     "biblio"
    #   ],
    #   "analyzer": "textIcu",
    #   "search_analyzer": "textIcuSearch"
    # },
    #
    body = {
        "query": {
            "match": {
                "name": {
                    "query": container.name,
                    "operator": "AND"
                }
            }
        },
        "size": size,
    }
    resp = es.search(body=body, index="fatcat_container")
    if resp["hits"]["total"] > 0:
        return response_to_entity_list(resp, entity_type=ContainerEntity, api=api)

    # Get fuzzy.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness
    body = {
        "query": {
            "match": {
                "name": {
                    "query": container.name,
                    "operator": "AND",
                    "fuzziness": "AUTO",
                }
            }
        },
        "size": size,
    }
    resp = es.search(body=body, index="fatcat_container")
    if resp["hits"]["total"] > 0:
        return response_to_entity_list(resp, entity_type=ContainerEntity, api=api)

    return []


def match_release_fuzzy(release: ReleaseEntity,
                        size: int = 5,
                        es: Optional[Union[str, elasticsearch.client.Elasticsearch]] = None,
                        api: Optional[DefaultApi] = None) -> List[ReleaseEntity]:
    """
    Given a release entity, return a number similar release entities from
    fatcat using Elasticsearch.
    """
    assert isinstance(release, ReleaseEntity)

    if size is None or size == 0:
        size = 10000  # or any large number

    if isinstance(es, str):
        es = elasticsearch.Elasticsearch([es])
    if es is None:
        es = elasticsearch.Elasticsearch()

    # Try to match by external identifier.
    ext_ids = release.ext_ids
    attrs = {
        "doi": "doi",
        "wikidata_qid": "wikidata_qid",
        "isbn13": "isbn13",
        "pmid": "pmid",
        "pmcid": "pmcid",
        "core": "code_id",
        "arxiv": "arxiv_id",
        "jstor": "jstor_id",
        "ark": "ark_id",
        "mag": "mag_id",
    }
    for attr, es_field in attrs.items():
        value = getattr(ext_ids, attr)
        if not value:
            continue
        s = (elasticsearch_dsl.Search(using=es,
                                      index="fatcat_release").query("term", **{
                                          es_field: value
                                      }).extra(size=size))
        resp = s.execute()
        if len(resp) > 0:
            return response_to_entity_list(resp, entity_type=ReleaseEntity, api=api)

    body = {
        "query": {
            "match": {
                "title": {
                    "query": release.title,
                    "operator": "AND"
                }
            }
        },
        "size": size,
    }
    resp = es.search(body=body, index="fatcat_release")
    if resp["hits"]["total"] > 0:
        return response_to_entity_list(resp, entity_type=ReleaseEntity, api=api)

    # Get fuzzy.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness
    body = {
        "query": {
            "match": {
                "title": {
                    "query": release.title,
                    "operator": "AND",
                    "fuzziness": "AUTO",
                }
            }
        },
        "size": size,
    }
    resp = es.search(body=body, index="fatcat_release")
    if resp["hits"]["total"] > 0:
        return response_to_entity_list(resp, entity_type=ReleaseEntity, api=api)

    return []


def verify_serial_name(a: str, b: str) -> MatchStatus:
    """
    Serial name verification. Serial names are a subset of container names.
    There are about 2M serials.
    """
    issnls_for_a = serialsdb.get(a, set())
    issnls_for_b = serialsdb.get(b, set())

    # If any name yields multiple ISSN-L, we cannot decide.
    if len(issnls_for_a) > 1:
        return MatchStatus.AMBIGIOUS
    if len(issnls_for_b) > 1:
        return MatchStatus.AMBIGIOUS

    # If both names point the same ISSN-L, it is an exact match.
    if len(issnls_for_a) == 1 and len(issnls_for_b) == 1:
        if len(issnls_for_a & issnls_for_b) == 1:
            return MatchStatus.EXACT
        else:
            return MatchStatus.DIFFERENT

    # Multiple names possible, but there is overlap.
    if len(issnls_for_a & issnls_for_b) > 0:
        return MatchStatus.STRONG

    return MatchStatus.AMBIGIOUS


def verify_container_name(a: str, b: str) -> MatchStatus:
    pass


def verify_container_match(a: ContainerEntity, b: ContainerEntity) -> MatchStatus:
    pass


def verify_release_match(a: ReleaseEntity, b: ReleaseEntity) -> MatchStatus:
    pass
