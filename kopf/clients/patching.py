from typing import Optional
from typing import cast

import aiohttp

from kopf.clients import auth
from kopf.clients import discovery
from kopf.structs import bodies
from kopf.structs import patches
from kopf.structs import resources


@auth.reauthenticated_request
async def patch_obj(
        *,
        resource: resources.Resource,
        patch: patches.Patch,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        body: Optional[bodies.Body] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Optional[bodies.RawBody]:
    """
    Patch a resource of specific kind.

    Either the namespace+name should be specified, or the body,
    which is used only to get namespace+name identifiers.

    Unlike the object listing, the namespaced call is always
    used for the namespaced resources, even if the operator serves
    the whole cluster (i.e. is not namespace-restricted).

    Returns the patched body. The patched body contains only the full body
    in case of a `body_patch`. In case of a `status_patch`, the patched body
    might contain only the `status` field. If none of the cases apply, the
    patched body is None.
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

    response_to_body_patch, response_to_status_patch = None, None
    try:
        if body_patch:
            response_to_body_patch = await context.session.patch(
                url=resource.get_url(server=context.server, namespace=namespace, name=name),
                headers={'Content-Type': 'application/merge-patch+json'},
                json=body_patch,
                raise_for_status=True,
            )
        if status_patch:
            response_to_status_patch = await context.session.patch(
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

    if (not response_to_status_patch) and (not response_to_body_patch):
        return None
    else:
        result_body = bodies.RawBody()
        if response_to_body_patch:
            result_body = await response_to_body_patch.json()
        if response_to_status_patch:
            status = await response_to_status_patch.json()
            result_body['status'] = status
        return result_body
