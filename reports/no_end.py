import argparse
import datetime
import json
import sys
import time

import prettytable

#sys.path.append("/stacktach")
sys.path.append("..")

from stacktach import datetime_to_decimal as dt
from stacktach import image_type
from stacktach import models


def make_report(yesterday=None, start_hour=0, hours=24, percentile=97,
                store=False, region=None):
    if not yesterday:
        yesterday = datetime.datetime.utcnow().date() - \
                    datetime.timedelta(days=1)

    rstart = datetime.datetime(year=yesterday.year, month=yesterday.month,
                              day=yesterday.day, hour=start_hour)
    rend = rstart + datetime.timedelta(hours=hours-1, minutes=59, seconds=59)

    dstart = dt.dt_to_decimal(rstart)
    dend = dt.dt_to_decimal(rend)

    codes = {}

    cells = []
    regions = []
    if region:
        region = region.upper()
    deployments = models.Deployment.objects.all()
    for deployment in deployments:
        name = deployment.name.upper()
        if not region or region in name:
            regions.append(deployment.id)
            cells.append(deployment.name)

    if not len(regions):
        print "No regions found for '%s'" % region
        sys.exit(1)

    # Get all the instances that have changed in the last N hours ...
    updates = models.RawData.objects.filter(event='compute.instance.update',
                                            when__gt=dstart, when__lte=dend,
                                            deployment__in=regions)\
                                    .values('instance').distinct()

    cmds = ['create', 'rebuild', 'rescue', 'resize.prep', 'resize.confirm', 
            'resize.revert',  'snapshot']
 
    good_operations = {}

    for uuid_dict in updates:
        uuid = uuid_dict['instance']

        # All the unique Request ID's for this instance during that timespan.
        reqs = models.RawData.objects.filter(instance=uuid,
                                             when__gt=dstart, when__lte=dend) \
                                     .values('request_id').distinct()


        for req_dict in reqs:
            req = req_dict['request_id']
            raws = models.RawData.objects.filter(request_id=req)\
                                      .exclude(event='compute.instance.exists')\
                                      .values('event', 'when', 'service', 
                                              'routing_key', 'state', 'old_state',
                                              'task', 'old_task')\
                                      .order_by('when')

            operation = None
            success = True
            chain = []
            for raw in raws:
                _routing_key = raw['routing_key']
                if 'error' in _routing_key:
                    success = False
                    break

                _event = raw['event']
                _service = raw['service']
                _state = raw['state']
                _old_state = raw['old_state']
                _task = raw['task']
                _old_task = raw['old_task']

                for cmd in cmds:
                    if cmd in _event:
                        operation = cmd
                        break

                chain.append((_event, _service, _old_state, _state, _old_task, _task))

            if not success:
                continue

            if not operation:
                continue

            key = (operation, tuple(chain))
            good_operations[key] = good_operations.get(key, 0) + 1

    key_count = sorted(good_operations.iteritems(), key=lambda kc: kc[1], reverse=True)
    final = []
    for cmd in cmds:
        for key, count in key_count:
            operation, chain = key
            if operation != cmd:
                continue
            final.append((operation, chain, count))

    for operation, chain, count in final:
        print "Operation: %s (%d)" % (operation, count)
        for x in chain:
            print "  %s %s %s->%s %s-> %s" % x


def valid_date(date):
    try:
        t = time.strptime(date, "%Y-%m-%d")
        return datetime.datetime(*t[:6])
    except Exception, e:
        raise argparse.ArgumentTypeError(
                                    "'%s' is not in YYYY-MM-DD format." % date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('StackTach Nova Usage Summary Report')
    parser.add_argument('--utcdate',
            help='Report start date YYYY-MM-DD. Default yesterday midnight.',
            type=valid_date, default=None)
    parser.add_argument('--region',
            help='Report Region. Default is all regions.', default=None)
    parser.add_argument('--hours',
            help='Report span in hours. Default: 24', default=24,
            type=int)
    parser.add_argument('--days_back',
            help='Report start date. N days back from now. Default: 0', default=0,
            type=int)
    parser.add_argument('--hours_back',
            help='Report start date. N hours back from now. Default: 0', default=0,
            type=int)
    parser.add_argument('--start_hour',
            help='Starting hour 0-23. Default: 0', default=0,
            type=int)
    parser.add_argument('--percentile',
            help='Percentile for timings. Default: 97', default=97,
            type=int)
    parser.add_argument('--store',
            help='Store report in database. Default: False',
            default=False, action="store_true")
    parser.add_argument('--silent',
            help="Do not show summary report. Default: False",
            default=False, action="store_true")
    args = parser.parse_args()

    yesterday = args.utcdate
    days_back = args.days_back
    hours_back = args.hours_back
    percentile = args.percentile
    hours = args.hours
    start_hour = args.start_hour
    store_report = args.store
    region = args.region

    if (not yesterday) and days_back > 0:
        yesterday = datetime.datetime.utcnow().date() - \
                    datetime.timedelta(days=days_back)
    if (not yesterday) and hours_back > 0:
        yesterday = datetime.datetime.utcnow() - \
                    datetime.timedelta(hours=hours_back)
        yesterday = yesterday.replace(minute=0, second=0, microsecond=0)
        start_hour = yesterday.hour

    make_report(yesterday, start_hour, hours,
                percentile, store_report, region)

