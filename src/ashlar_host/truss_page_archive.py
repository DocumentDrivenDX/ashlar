# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original bounded public Truss page bytes; never live source authority."""
import base64
import hashlib
from .delta_custody import encoded
from .private_graph_custody import decode_custody_bytes, require_custody_digest
from ashlar.truss_native_feed import native_feed_batch, MAX_NATIVE_PAGE_BYTES

PROFILE = 'ashlar-truss-original-feed-page/0.1'
MAXIMUM_ARCHIVE_BYTES = 3*1024*1024
MAXIMUM_CONTENT_BASE64_BYTES = 4*((MAX_NATIVE_PAGE_BYTES+2)//3)
MAXIMUM_CURSOR_BASE64_BYTES = 4*((16384+2)//3)


def _types():
    from truss.feed import FeedPage, FeedCursor
    return FeedPage, FeedCursor


def encode_truss_page(page, feed):
    FeedPage, _ = _types()
    if type(page) is not FeedPage:
        raise ValueError('Original genuine public Truss FeedPage required')
    if type(page.content)is not bytes or not 1<=len(page.content)<=MAX_NATIVE_PAGE_BYTES:
        raise ValueError('Original Truss page must fit retained byte-carrier bounds')
    batch = native_feed_batch(page.content, sha256=page.sha256, prior=page.prior.content,
        next=page.next.content, feed=feed)
    cursor = decode_custody_bytes(page.prior.content)
    writer=batch.batch_id if page.outcome=='transaction' else None
    if (page.consumer_id != cursor['consumerId'] or page.registration_id != cursor['registrationId']
            or page.outcome not in ('transaction','empty_interval') or page.writer_xid != writer
            or (page.outcome=='empty_interval')!=(batch.batch_id.startswith('empty-interval:'))):
        raise ValueError('Original Truss page registration/writer differs')
    if page.outcome=='empty_interval' and page.safe_watermark_xid!=decode_custody_bytes(page.next.content)['nextXid']:
        raise ValueError('Original empty interval safe frontier differs')
    value = {'profile': PROFILE, 'feed': feed, 'content': base64.b64encode(page.content).decode(),
        'prior': base64.b64encode(page.prior.content).decode(), 'next': base64.b64encode(page.next.content).decode(),
        'sha256': page.sha256, 'consumer_id': page.consumer_id, 'registration_id': page.registration_id,
        'writer_xid': page.writer_xid, 'safe_watermark_xid': page.safe_watermark_xid, 'outcome': page.outcome}
    if (len(value['content'])>MAXIMUM_CONTENT_BASE64_BYTES
            or any(len(value[field])>MAXIMUM_CURSOR_BASE64_BYTES for field in ('prior','next'))):
        raise ValueError('Original Truss page must fit retained byte-carrier bounds')
    raw = (encoded(value)+'\n').encode()
    if len(raw) > MAXIMUM_ARCHIVE_BYTES:
        raise ValueError('Bounded original Truss page archive required')
    return raw


def decode_truss_page(raw, expected_sha256, feed):
    require_custody_digest(expected_sha256)
    if type(raw) is not bytes or not 0 < len(raw) <= MAXIMUM_ARCHIVE_BYTES or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('Original Truss page archive digest differs')
    value = decode_custody_bytes(raw)
    fields = {'profile', 'feed', 'content', 'prior', 'next', 'sha256', 'consumer_id', 'registration_id', 'writer_xid', 'safe_watermark_xid', 'outcome'}
    if type(value) is not dict or set(value) != fields or value['profile'] != PROFILE or value['feed'] != feed or (encoded(value)+'\n').encode() != raw:
        raise ValueError('Closed canonical original Truss page archive required')
    decoded = {}
    for field in ('content', 'prior', 'next'):
        text = value[field]
        if type(text) is not str or not text or len(text) > (MAXIMUM_CONTENT_BASE64_BYTES if field=='content'else MAXIMUM_CURSOR_BASE64_BYTES):
            raise ValueError('Bounded original Truss page byte carrier required')
        decoded[field] = base64.b64decode(text, validate=True)
        if base64.b64encode(decoded[field]).decode() != text:
            raise ValueError('Canonical original Truss page byte carrier required')
    for field in ('feed', 'sha256', 'consumer_id', 'registration_id', 'safe_watermark_xid', 'outcome'):
        if type(value[field]) is not str or not value[field] or len(value[field]) > 256:
            raise ValueError('Original bounded Truss page scalar required')
    if value['writer_xid'] is not None and (type(value['writer_xid']) is not str or not value['writer_xid'] or len(value['writer_xid'])>256):
        raise ValueError('Original bounded Truss page writer required')
    FeedPage, FeedCursor = _types()
    page = FeedPage(content=decoded['content'], sha256=value['sha256'], prior=FeedCursor(content=decoded['prior']),
        next=FeedCursor(content=decoded['next']), consumer_id=value['consumer_id'], registration_id=value['registration_id'],
        writer_xid=value['writer_xid'], safe_watermark_xid=value['safe_watermark_xid'], outcome=value['outcome'])
    if encode_truss_page(page, feed=value['feed']) != raw:
        raise ValueError('Original public Truss page reconstruction differs')
    return page
