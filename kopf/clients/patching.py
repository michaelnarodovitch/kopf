import copy
from typing import Optional, cast, Any, Union

import aiohttp

from kopf.clients import auth
from kopf.clients import discovery
from kopf.structs import bodies, diffs
from kopf.structs import patches
from kopf.structs import resources

from logging import getLogger

logger = getLogger('kopf.clients.patching')


@auth.reauthenticated_request
async def patch_obj(
        *,
        resource: resources.Resource,
        patch: patches.Patch,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        body: Optional[bodies.Body] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> None:
    """
    Patch a resource of specific kind.

    Either the namespace+name should be specified, or the body,
    which is used only to get namespace+name identifiers.

    Unlike the object listing, the namespaced call is always
    used for the namespaced resources, even if the operator serves
    the whole cluster (i.e. is not namespace-restricted).
    """
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    if body is not None and (name is not None or namespace is not None):
        raise TypeError("Either body, or name+namespace can be specified. Got both.")

    namespace = body.get('metadata', {}).get('namespace') if body is not None else namespace
    name = body.get('metadata', {}).get('name') if body is not None else name

    is_namespaced = await discovery.is_namespaced(resource=resource, context=context)
    namespace = namespace if is_namespaced else None

    if body is None:
        body = cast(bodies.Body, {'metadata': {'name': name}})
        if namespace is not None:
            body['metadata']['namespace'] = namespace

    as_subresource = await discovery.is_status_subresource(resource=resource, context=context)
    body_patch = dict(patch)  # shallow: for mutation of the top-level keys below.
    status_patch = body_patch.pop('status', None) if as_subresource else None

    try:
        if body_patch:
            url = resource.get_url(server=context.server, namespace=namespace, name=name)
            resp = await context.session.patch(
                url=url,
                headers={'Content-Type': 'application/merge-patch+json'},
                json=body_patch,
                raise_for_status=True,
            )
            new_body = await resp.json()
            if not _patch_is_merged(body, new_body, patch):
                logger.debug(f'Patch on {url} was not merged. Potentially due to '
                             f'schema mismatch or a mutating admission controller.')

        if status_patch:
            await context.session.patch(
                url=resource.get_url(server=context.server, namespace=namespace, name=name,
                                     subresource='status' if as_subresource else None),
                headers={'Content-Type': 'application/merge-patch+json'},
                json={'status': status_patch},
                raise_for_status=True,
            )

    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            pass
        else:
            raise


def _patch_is_merged(old: Optional[bodies.Body], new: bodies.Body, patch: dict):
    if old:
        expected = _merge_patch(copy.deepcopy(old), patch)
        return new == expected
    else:
        return True


def _merge_patch(target: Union[dict, int, str, float, None],
                 patch: Union[dict, int, str, float, None]):
    """
    An implementation of https://tools.ietf.org/rfc/rfc7386.txt for merge-patch+json
    """
    if isinstance(patch, dict):
        if not isinstance(target, dict):
            target = {}
        for k, v in patch.items():
            if k in target:
                if v is None:
                    del target[k]
                else:
                    target[k] = _merge_patch(target[k], v)
            else:
                target[k] = v
        return target
    else:
        return patch
