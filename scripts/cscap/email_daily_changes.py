"""
  My purpose in life is to send an email each day with changes found
  on the Google Drive
"""
from __future__ import print_function
import sys
import datetime
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from gdata.client import RequestError
import pytz
import pyiem.cscap_utils as util

CONFIG = util.get_config()
FMIME = 'application/vnd.google-apps.folder'
FORM_MTYPE = 'application/vnd.google-apps.form'
SITES_MYTPE = 'application/vnd.google-apps.site'
CFG = {'cscap': dict(emails=CONFIG['cscap']['email_daily_list'],
                     title="Sustainable Corn"),
       'inrc': dict(
            emails=CONFIG['inrc']['email_daily_list'],
            title='Integrating Social and Biophysical Indicators (ISBI)'),
       'ardn': dict(
            emails=CONFIG['ardn']['email_daily_list'],
            title='Ag Research Data Network'),
       'kb': dict(
            emails=CONFIG['kb']['email_daily_list'],
            title='Knowledgebase (2019-2023)'),
       'nutrinet': dict(emails=CONFIG['nutrinet']['email_daily_list'],
                        title='NutriNet (4R)'),
       'td': dict(emails=CONFIG['td']['email_daily_list'],
                  title='Transforming Drainage')}
LOCALTZ = pytz.timezone("America/Chicago")


def pprint(mydict):
    """pretty print JSON"""
    return json.dumps(mydict, sort_keys=True, indent=4,
                      separators=(',', ': '))


def sites_changelog(regime, yesterday, html):
    """Do Sites Changelog"""
    html += """
    <h4>%s Internal Website Changes</h4>
    <table border="1" cellpadding="3" cellspacing="0">
    <thead><tr><th>Time</th><th>Activity</th></tr></thead>
    <tbody>""" % (CFG[regime]['title'],)

    if regime == 'cscap':
        site = 'sustainablecorn'
    elif regime == 'td':
        site = 'transformingdrainage'
    else:
        site = 'nutrinet'
    s = util.get_sites_client(CONFIG, site)
    # Fetch more results for sites activity feed
    opt = {'max-results': 999}
    try:
        feed = s.get_activity_feed(**opt)
    except RequestError:
        html += (
            "<tr><th colspan=\"2\">Google Sites API Error :(</th></tr>"
            "</tbody></table>"
        )
        return html
    tablerows = []
    for entry in feed.entry:
        ts = datetime.datetime.strptime(entry.updated.text,
                                        '%Y-%m-%dT%H:%M:%S.%fZ')
        ts = ts.replace(tzinfo=pytz.UTC)
        # print 'Sites ts: %s' % (ts,)
        if ts < yesterday:
            continue
        updated = ts.astimezone(LOCALTZ)
        elem = entry.summary.html
        elem.namespace = ''
        elem.children[0].namespace = ''
        tablerows.append(("<tr><td>%s</td><td>%s %s</td></tr>\n"
                          ) % (updated.strftime("%-d %b %-I:%M %P"),
                               elem.text, str(elem.children[0])))

    if tablerows:
        tablerows.append("<tr><td colspan='2'>No Changes Found</td></tr>")

    html += "".join(tablerows)
    html += """</tbody></table>"""
    return html


def drive_changelog(regime, yesterday, html):
    """ Do something """
    drive = util.get_driveclient(CONFIG, regime)
    folders = util.get_folders(drive)
    start_change_id = CONFIG[regime]["changestamp"]

    html += """<p><table border="1" cellpadding="3" cellspacing="0">
<thead>
<tr><th>Folder</th><th>Resource</th></tr>
</thead>
<tbody>"""

    largestChangeId = -1
    hits = 0
    page_token = None
    changestamp = None
    param = {'includeDeleted': False, 'maxResults': 1000}
    while True:
        if start_change_id:
            param['startChangeId'] = start_change_id
        if page_token:
            param['pageToken'] = page_token
        print(("[%s] start_change_id: %s largestChangeId: %s page_token: %s"
               ) % (regime, start_change_id, largestChangeId, page_token))
        response = drive.changes().list(**param).execute()
        largestChangeId = response['largestChangeId']
        page_token = response.get('nextPageToken')
        for item in response['items']:
            if item['file']['mimeType'] in [FMIME, FORM_MTYPE, SITES_MYTPE]:
                continue
            changestamp = item['id']
            if item['deleted']:
                continue
            # don't do more work when this file actually did not change
            modifiedDate = datetime.datetime.strptime(
                item['file']['modifiedDate'][:19], '%Y-%m-%dT%H:%M:%S')
            modifiedDate = modifiedDate.replace(tzinfo=pytz.timezone("UTC"))
            if modifiedDate < yesterday:
                continue
            # Need to see which base folder this file is in!
            isproject = False
            for parent in item['file']['parents']:
                if parent['id'] not in folders:
                    print(('[%s] file: %s has unknown parent: %s'
                           ) % (regime, item['id'], parent['id']))
                    continue
                isproject = True
            if not isproject:
                print(('[%s] %s (%s) skipped as basefolders are: %s'
                       ) % (regime, repr(item['file']['title']),
                            item['file']['mimeType'],
                            item['file']['parents']))
                continue
            uri = item['file']['alternateLink']
            title = item['file']['title'].encode(
                'ascii', 'ignore').decode('ascii')
            localts = modifiedDate.astimezone(LOCALTZ)
            hits += 1
            pfolder = item['file']['parents'][0]['id']
            html += """
<tr>
<td><a href="https://docs.google.com/folderview?id=%s&usp=drivesdk">%s</a></td>
<td><a href="%s">%s</a></td></tr>
            """ % (pfolder, folders[pfolder]['title'], uri, title)
            hit = False
            if 'version' in item['file']:
                lastmsg = ""
                try:
                    revisions = drive.revisions().list(
                        fileId=item['file']['id']).execute()
                except Exception:
                    print(('[%s] file %s (%s) failed revisions'
                           ) % (regime, title, item['file']['mimeType']))
                    revisions = {'items': []}
                for item2 in revisions['items']:
                    # print pprint(item2)
                    md = datetime.datetime.strptime(
                                                    item2['modifiedDate'][:19],
                                                    '%Y-%m-%dT%H:%M:%S')
                    md = md.replace(tzinfo=pytz.timezone("UTC"))
                    if md < yesterday:
                        continue
                    localts = md.astimezone(LOCALTZ)
                    # for some reason, some revisions have no user associated
                    # with it.  So just skip for now
                    # http://stackoverflow.com/questions/1519072
                    if 'lastModifyingUser' not in item2:
                        continue
                    luser = item2['lastModifyingUser']
                    hit = True
                    display_name = luser['displayName']
                    email_address = luser['emailAddress']
                    if display_name == CONFIG['service_account']:
                        display_name = "daryl's magic"
                        email_address = "akrherz@iastate.edu"
                    thismsg = """
    <tr><td colspan="2"><img src="%s" style="height:25px;"/> %s by
     %s (%s)</td></tr>
                    """ % ((luser['picture']['url']
                            if 'picture' in luser else ''),
                           localts.strftime("%-d %b %-I:%M %p"),
                           display_name, email_address)
                    if thismsg != lastmsg:
                        html += thismsg
                    lastmsg = thismsg
            # Now we check revisions
            if not hit:
                luser = item['file'].get('lastModifyingUser', dict())
                html += """
<tr><td colspan="2"><img src="%s" style="height:25px;"/> %s by
 %s (%s)</td></tr>
                """ % (luser['picture']['url'] if 'picture' in luser else '',
                       localts.strftime("%-d %b %-I:%M %p"),
                       luser.get('displayName', 'n/a'),
                       luser.get('emailAddress', 'n/a'))
        if not page_token:
            break

    if changestamp is not None:
        CONFIG[regime]['changestamp'] = changestamp
    if hits == 0:
        html += """<tr><td colspan="5">No Changes Found...</td></tr>\n"""

    html += """</tbody></table>"""

    util.save_config(CONFIG)
    return html


def main(argv):
    """Do Fun things"""
    regime = argv[1]

    today = datetime.datetime.utcnow()
    today = today.replace(tzinfo=pytz.UTC, hour=12,
                          minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    localts = yesterday.astimezone(LOCALTZ)
    html = """
<h3>%s Cloud Data Changes</h3>
<br />
<p>Period: %s - %s

<h4>Google Drive File Changes</h4>
""" % (CFG[regime]['title'],
       localts.strftime("%-I %p %-d %B %Y"),
       (localts + datetime.timedelta(hours=24)).strftime("%-I %p %-d %B %Y"))

    html = drive_changelog(regime, yesterday, html)
    # if regime != 'nutrinet':
    #    html = sites_changelog(regime, yesterday, html)

    html += """<p>That is all...</p>"""
    # debugging
    if len(sys.argv) == 3:
        ofp = open('/tmp/out.html', 'w')
        ofp.write(html)
        ofp.close()
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "%s %s Data ChangeLog" % (yesterday.strftime("%-d %b"),
                                               CFG[regime]['title'])
    msg['From'] = 'mesonet@mesonet.agron.iastate.edu'
    msg['To'] = ','.join(CFG[regime]['emails'])

    part2 = MIMEText(html, 'html')

    msg.attach(part2)

    p25 = smtplib.SMTP('mailhub.iastate.edu')
    p25.sendmail(msg['From'], CFG[regime]['emails'], msg.as_string())
    p25.quit()


if __name__ == '__main__':
    main(sys.argv)
