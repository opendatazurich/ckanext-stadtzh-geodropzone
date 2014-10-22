# coding: utf-8

import os
import time
import datetime
import difflib
from lxml import etree
from pprint import pprint

from ofs import get_impl
from pylons import config
from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name, munge_filename
from ckanext.harvest.harvesters.base import munge_tag

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, HarvestObjectError
from ckanext.stadtzhharvest.harvester import StadtzhHarvester

from pylons import config

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

        ids = []

        # list directories in geodropzone folder
        datasets = self._remove_hidden_files(os.listdir(self.DROPZONE_PATH))

        # foreach -> meta.xml -> create entry
        for dataset in datasets:
            with open(os.path.join(self.DROPZONE_PATH, dataset, 'DEFAULT/meta.xml'), 'r') as meta_xml:
                parser = etree.XMLParser(encoding='utf-8')
                dataset_node = etree.fromstring(meta_xml.read(), parser=parser).find('datensatz')

                metadata = {
                    'datasetID': dataset,
                    'title': dataset_node.find('titel').text,
                    'url': self._get(dataset_node, 'lieferant'),
                    'notes': dataset_node.find('beschreibung').text,
                    'author': dataset_node.find('quelle').text,
                    'maintainer': 'Open Data Zürich',
                    'maintainer_email': 'opendata@zuerich.ch',
                    'license_id': 'cc-zero',
                    'license_url': 'http://opendefinition.org/licenses/cc-zero/',
                    'tags': self._generate_tags(dataset_node),
                    'groups': self._get(dataset_node, 'kategorie'),
                    'resources': self._generate_resources_dict_array(dataset + '/DEFAULT'),
                    'extras': [
                            ('spatialRelationship', self._get(dataset_node, 'raeumliche_beziehung')),
                            ('dateFirstPublished', self._get(dataset_node, 'erstmalige_veroeffentlichung')),
                            ('dateLastUpdated', self._get(dataset_node, 'aktualisierungsdatum')),
                            ('updateInterval', self._get(dataset_node, 'aktualisierungsintervall').replace(u'ä', u'ae').replace(u'ö', u'oe').replace(u'ü', u'ue')),
                            ('dataType', self._get(dataset_node, 'datentyp')),
                            ('legalInformation', self._get(dataset_node, 'rechtsgrundlage')),
                            ('version', self._get(dataset_node, 'aktuelle_version')),
                            ('timeRange', self._get(dataset_node, 'zeitraum')),
                            ('comments', self._convert_comments(dataset_node)),
                            ('attributes', self._json_encode_attributes(self._get_attributes(dataset_node))),
                            ('dataQuality', self._get(dataset_node, 'datenqualitaet'))
                    ],
                    'related': self._get_related(dataset_node)
                }

                # Get group IDs from group titles
                user = model.User.get(self.config['user'])
                context = {
                    'model': model,
                    'session': Session,
                    'user': self.config['user']
                }

                if metadata['groups']:
                    groups = []
                    group_titles = metadata['groups'].split(', ')
                    for title in group_titles:
                        if title == u'Bauen und Wohnen':
                            name = u'bauen-wohnen'
                        else:
                            name = title.lower().replace(u'ö', u'oe').replace(u'ä', u'ae')
                        try:
                            data_dict = {'id': name}
                            group_id = get_action('group_show')(context, data_dict)['id']
                            groups.append(group_id)
                            log.debug('Added group %s' % name)
                        except:
                            data_dict['name'] = name
                            data_dict['title'] = title
                            log.debug('Couldn\'t get group id. Creating the group `%s` with data_dict: %s', name, data_dict)
                            group_id = get_action('group_create')(context, data_dict)['id']
                            groups.append(group_id)
                    metadata['groups'] = groups
                else:
                    metadata['groups'] = []
                    log.debug('No groups found for dataset %s.' % dataset)

                for extra in list (metadata['extras']):
                    if extra[0] == 'updateInterval' or extra[0] == 'dataType':
                        if not extra[1]:
                            metadata['extras'].append((extra[0], '   '))
                            metadata['extras'].remove(extra)
                            log.debug('No value in meta.xml for %s' % extra[0])

                obj = HarvestObject(
                    guid = metadata['datasetID'],
                    job = harvest_job,
                    content = json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + metadata['datasetID'] + ' to the queue')
                ids.append(obj.id)

                if not os.path.isdir(os.path.join(self.METADATA_PATH, dataset)):
                    os.makedirs(os.path.join(self.METADATA_PATH, dataset))

                with open(os.path.join(self.METADATA_PATH, dataset, 'metadata-' + str(datetime.date.today())), 'w') as meta_json:
                    meta_json.write(json.dumps(metadata, sort_keys=True, indent=4, separators=(',', ': ')))
                    log.debug('Metadata JSON created')

        return ids


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
                self._create_diffs(package_dict)
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
