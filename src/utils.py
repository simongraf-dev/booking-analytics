"""
Utility functions for data transformation
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from datetime import datetime

def unix_to_datetime(unix_ms):
    """Convert Unix milliseconds to datetime"""
    if unix_ms is None:
        return None
    return datetime.fromtimestamp(unix_ms / 1000)

def parse_booking(raw_booking):
    """Convert GraphQL booking to PostgreSQL format"""
    return {
        'id': raw_booking['_id'],
        'booking_date': unix_to_datetime(raw_booking['date']),
        'end_date': unix_to_datetime(raw_booking['endDate']),
        'people': raw_booking['people'],
        'cancelled': raw_booking.get('cancelled') or False,
        'no_show': raw_booking.get('noShow') or False,
        'walk_in': raw_booking.get('walkIn') or False,
        'source': raw_booking.get('source'),
        'host': raw_booking.get('host'),
        'tracking': json.dumps(raw_booking.get('tracking')) if raw_booking.get('tracking') else None,
        'tag_ids': raw_booking.get('tagIds') or [],
        'booking_tags_count': json.dumps(raw_booking.get('bookingTagsCount')) if raw_booking.get('bookingTagsCount') else None,
        'payment': json.dumps(raw_booking.get('payment')) if raw_booking.get('payment') else None,
        'rating': raw_booking.get('rating')
    }