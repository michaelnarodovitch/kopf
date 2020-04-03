from unittest.mock import Mock
import logging as logging_
import pytest

from kopf.engines import logging
from kopf.reactor import processing
from kopf.structs import bodies
from kopf.structs import patches


@pytest.fixture()
def logger(settings):
    settings.posting.level = logging_.DEBUG
    return logging.ObjectLogger(
        body=bodies.Body({
            'apiVersion': 'group1/version1',
            'kind': 'Kind1',
            'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}),
        settings=settings)


@pytest.mark.parametrize('patch,new_body,mismatch', [
    pytest.param({'status': {'akey': 'avalue'}}, {},
                 [{'path': 'status.akey', 'got': None, 'expected': 'avalue'}]),
    pytest.param({'status': {'akey': {'bkey': 'avalue'}}}, {},
                 [{'path': 'status.akey.bkey', 'got': None, 'expected': 'avalue'}]),
    pytest.param({'status': {'akey': {'bkey': {'ckey': 'avalue'}}}}, {},
                 [{'path': 'status.akey.bkey.ckey', 'got': None, 'expected': 'avalue'}]),
    pytest.param({'status': {'akey': 'avalue'}}, {'status': {'akey': 'bvalue'}},
                 [{'path': 'status.akey', 'got': 'bvalue', 'expected': 'avalue'}]),
    pytest.param({'status': {'akey': None}}, {'status': {'akey': 'avalue'}},
                 [{'path': 'status.akey', 'got': 'avalue', 'expected': None}]),
    pytest.param({'status': {'akey': 'avalue'}}, {'status': {'akey': 'bvalue', 'bkey': 'cvalue'}},
                 [{'path': 'status.akey', 'got': 'bvalue', 'expected': 'avalue'}]),
])
async def test_apply_reaction_outcomes_mismatch(patch: patches.Patch,
                                                new_body: bodies.Body,
                                                mismatch: dict,
                                                resource,
                                                patcher_mock,
                                                logger,
                                                caplog):
    caplog.set_level(logging_.DEBUG)
    body = bodies.Body({})
    delays = []
    replenished = Mock()
    patcher_mock.return_value = new_body

    await processing.apply_reaction_outcomes(
        resource=resource,
        body=body,
        patch=patch,
        delays=delays,
        logger=logger,
        replenished=replenished
    )

    assert caplog.messages == [
        f'Patching with: {patch}',
        f'Patched body does not match with patch: {mismatch}'
    ]
