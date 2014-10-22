# coding: utf-8

import os
import difflib

from pylons import config
from ckan import model
from ckan.model import Session
from ckan.logic import get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name, munge_filename
from ckanext.stadtzhharvest.harvester import StadtzhHarvester

import logging
log = logging.getLogger(__name__)


class StadtzhgeodropzoneHarvester(StadtzhHarvester):
    '''
    The harvester for the Stadt ZH GEO Dropzone
    '''

    DROPZONE_PATH = '/usr/lib/ckan/GEO'
    METADATA_PATH = config.get('metadata.metadatapath', '/usr/lib/ckan/diffs/geo-metadata')

    def info(self):
        '''
        Return some general info about this harvester
        '''
        return {
            'name': 'stadtzhgeodropzone',
            'title': 'Stadtzhgeodropzone',
            'description': 'Harvests the Stadtzhgeodropzone data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In StadtzhgeodropzoneHarvester gather_stage')
        return self._gather_datasets(harvest_job)

    def fetch_stage(self, harvest_object):
        log.debug('In StadtzhgeodropzoneHarvester fetch_stage')

        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)

    def import_stage(self, harvest_object):
        log.debug('In StadtzhgeodropzoneHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)
            package_dict['id'] = harvest_object.guid
            package_dict['name'] = munge_title_to_name(package_dict[u'datasetID'])

            user = model.User.get(self.config['user'])
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }

            # Find or create the organization the dataset should get assigned to.
            try:
                data_dict = {
                    'permission': 'edit_group',
                    'id': munge_title_to_name(self.ORGANIZATION['de']),
                    'name': munge_title_to_name(self.ORGANIZATION['de']),
                    'title': self.ORGANIZATION['de']
                }
                package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
            except:
                organization = get_action('organization_create')(context, data_dict)
                package_dict['owner_org'] = organization['id']

            # Insert the package only when it's not already in CKAN, but move the resources anyway.
            package = model.Package.get(package_dict['id'])
            if package: # package has already been imported.
                # create a diff between this new metadata set and the one from yesterday.
                # send the diff to SSZ

                today = datetime.date.today()
                new_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-' + str(today))
                prev_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-previous')
                diff_path = os.path.join(self.DIFF_PATH, str(today) + '-' + package_dict['id'] + '.html')

                if not os.path.isdir(self.DIFF_PATH):
                    os.makedirs(self.DIFF_PATH)

                if os.path.isfile(new_metadata_path):
                    if os.path.isfile(prev_metadata_path):
                        with open(prev_metadata_path) as prev_metadata:
                            with open(new_metadata_path) as new_metadata:
                                if prev_metadata.read() != new_metadata.read():
                                    with open(prev_metadata_path) as prev_metadata:
                                        with open(new_metadata_path) as new_metadata:
                                            with open(diff_path, 'w') as diff:
                                                diff.write(
                                                    "<!DOCTYPE html>\n<html>\n<body>\n<h2>Metadata diff for the dataset <a href=\""
                                                    + self.INTERNAL_SITE_URL + "/dataset/" + package_dict['id'] + "\">"
                                                    + package_dict['id'] + "</a></h2></body></html>\n"
                                                )
                                                d = difflib.HtmlDiff(wrapcolumn=60)
                                                umlauts = {
                                                    "\\u00e4": "ä",
                                                    "\\u00f6": "ö",
                                                    "\\u00fc": "ü",
                                                    "\\u00c4": "Ä",
                                                    "\\u00d6": "Ö",
                                                    "\\u00dc": "Ü",
                                                    "ISO-8859-1": "UTF-8"
                                                }
                                                html = d.make_file(prev_metadata, new_metadata, context=True, numlines=1)
                                                for code in umlauts.keys():
                                                    html = html.replace(code, umlauts[code])
                                                diff.write(html)
                                                log.debug('Metadata diff generated for the dataset: ' + package_dict['id'])
                                else:
                                    log.debug('No change in metadata for the dataset: ' + package_dict['id'])
                        os.remove(prev_metadata_path)
                        log.debug('Deleted previous day\'s metadata file.')
                    else:
                        log.debug('No earlier metadata JSON')

                    os.rename(new_metadata_path, prev_metadata_path)

                else:
                    log.debug(new_metadata_path + ' Metadata JSON missing for the dataset: ' + package_dict['id'])

            else: # package does not exist, therefore create it.
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            # Move file around and make sure it's in the file-store
            for r in package_dict['resources']:
                old_filename = r['name']
                r['name'] = munge_filename(r['name'])
                if r['resource_type'] == 'file':
                    label = package_dict['datasetID'] + '/' + r['name']
                    file_contents = ''
                    with open(os.path.join(self.DROPZONE_PATH, package_dict['datasetID'], 'DEFAULT', old_filename)) as contents:
                        file_contents = contents.read()
                    params = {
                        'filename-original': 'the original file name',
                        'uploaded-by': self.config['user']
                    }
                    r['url'] = self.CKAN_SITE_URL + '/storage/f/' + label
                    self.get_ofs().put_stream(self.BUCKET, label, file_contents, params)

            if not package:
                result = self._create_or_update_package(package_dict, harvest_object)
                self._related_create_or_update(package_dict['name'], package_dict['related'])
                Session.commit()

        except Exception, e:
            log.exception(e)

        return True
