#coding: utf-8

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
from ckan.lib.munge import munge_title_to_name

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

from pylons import config

import logging
log = logging.getLogger(__name__)

class StadtzhgeodropzoneHarvester(HarvesterBase):
    '''
    The harvester for the Stadt ZH GEO Dropzone
    '''

    ORGANIZATION = {
        'de': u'Stadt Zürich',
        'fr': u'fr_Stadt Zürich',
        'it': u'it_Stadt Zürich',
        'en': u'en_Stadt Zürich',
    }
    LANG_CODES = ['de', 'fr', 'it', 'en']
    BUCKET = config.get('ckan.storage.bucket', 'default')
    CKAN_SITE_URL = config.get('ckan.site_url', 'http://stadtzh.lo')

    config = {
        'user': u'harvest'
    }

    DROPZONE_PATH = '/usr/lib/ckan/GEO'
    METADATA_PATH = '/vagrant/data/METADATA'

    # ---
    # COPIED FROM THE CKAN STORAGE CONTROLLER
    # ---

    def create_pairtree_marker(self, folder):
        """ Creates the pairtree marker for tests if it doesn't exist """
        if not folder[:-1] == '/':
            folder = folder + '/'

        directory = os.path.dirname(folder)
        if not os.path.exists(directory):
            os.makedirs(directory)

        target = os.path.join(directory, 'pairtree_version0_1')
        if os.path.exists(target):
            return

        open(target, 'wb').close()


    def get_ofs(self):
        """Return a configured instance of the appropriate OFS driver.
        """
        storage_backend = config['ofs.impl']
        kw = {}
        for k, v in config.items():
            if not k.startswith('ofs.') or k == 'ofs.impl':
                continue
            kw[k[4:]] = v

        # Make sure we have created the marker file to avoid pairtree issues
        if storage_backend == 'pairtree' and 'storage_dir' in kw:
            self.create_pairtree_marker(kw['storage_dir'])

        ofs = get_impl(storage_backend)(**kw)
        return ofs

    # ---
    # END COPY
    # ---

    def _remove_hidden_files(self, file_list):
        '''
        Removes dotfiles from a list of files
        '''
        cleaned_file_list = []
        for file in file_list:
            if not file.startswith('.'):
                cleaned_file_list.append(file)
        return cleaned_file_list


    def _generate_tags(self, dataset_node):
        '''
        Given a dataset node it extracts the tags and returns them in an array
        '''
        if dataset_node.find('keywords').text is not None:
            return dataset_node.find('keywords').text.split(', ')
        else:
            return []


    def _generate_resources_dict_array(self, dataset):
        '''
        Given a dataset folder, it'll return an array of resource metadata
        '''
        resources = []
        resource_files = self._remove_hidden_files((f for f in os.listdir(os.path.join(self.DROPZONE_PATH, dataset)) 
            if os.path.isfile(os.path.join(self.DROPZONE_PATH, dataset, f))))
        log.debug(resource_files)

        # for resource_file in resource_files:
        for resource_file in (x for x in resource_files if x != 'meta.xml'):
            if resource_file == u'link.xml':
                with open(os.path.join(self.DROPZONE_PATH, dataset, resource_file), 'r') as links_xml:
                    parser = etree.XMLParser(encoding='utf-8')
                    links = etree.fromstring(links_xml.read(), parser=parser).findall('link')
                    for link in links:
                        if link.find('url').text != "" and link.find('url').text != None:
                            resources.append({
                                'url': link.find('url').text,
                                'name': link.find('lable').text,
                                'format': link.find('type').text,
                                'resource_type': 'api'
                            })
            else:
                resources.append({
                    # 'url': '', # will be filled in the import stage
                    'name': resource_file,
                    'format': resource_file.split('.')[-1],
                    'resource_type': 'file'
                })

        return resources


    def _generate_attribute_notes(self, attributlist_node):
        '''
        Compose the attribute notes for all the given attributes
        '''
        response = u'##Attribute  \n'
        for attribut in attributlist_node:
            response += u'**' + attribut.find('sprechenderfeldname').text + u'**  \n'
            if attribut.find('feldbeschreibung').text != None:
                response += attribut.find('feldbeschreibung').text + u'  \n'
        return response

    def _node_exists_and_is_nonempty(self, dataset_node, element_name):
        element = dataset_node.find(element_name)
        if element == None:
            log.debug('TODO: send a message to SSZ, telling them Georg has to fix the meta.xml (OGDZH-29)')
            return None
        elif element.text == None:
            return None
        else:
            return element.text

    def _get(self, node, name):
        element = self._node_exists_and_is_nonempty(node, name)
        if element:
            return element
        else:
            return ''

    def _generate_notes(self, dataset_node, dataset_name):
        '''
        Compose the notes given the elements available within the node
        '''
        response = u''

        # details
        element_text = self._node_exists_and_is_nonempty(dataset_node, 'beschreibung')
        if element_text != None:
            response += u'**Details**  \n' + element_text + u'  \n'

        response += u'**Urheber**  \n' + u'  \n'
        response += u'**Erstmalige Veröffentlichung**  \n' + u'  \n'

        # zeitraum
        element_text = self._node_exists_and_is_nonempty(dataset_node, 'zeitraum')
        if element_text != None:
            response += u'**Zeitraum**  \n' + element_text + u'  \n'

        response += u'**Aktualisierungsintervall**  \n' + u'  \n'

        # aktuelle_version
        element_text = self._node_exists_and_is_nonempty(dataset_node, 'aktuelle_version')
        if element_text != None:
            response += u'**Aktuelle Version**  \n' + element_text + u'  \n'

        resources_path = os.path.join(self.DROPZONE_PATH, dataset_name, 'DEFAULT')
        resource_files = [f for f in os.listdir(resources_path) if not (f != 'meta.xml' or f.endswith(".txt"))]
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(os.path.join(resources_path, resource_files[0]))
        response += u'**Aktualisierungsdatum**  \n' + time.strftime('%d.%m.%Y, %H:%M Uhr', time.localtime(mtime)) + u'  \n'

        response += u'**Datentyp**  \n' + u'  \n'

        # quelle
        element_text = self._node_exists_and_is_nonempty(dataset_node, 'quelle')
        if element_text != None:
            response += u'**Quelle**  \n' + element_text + u'  \n'

        # raeumliche_beziehung
        element_text = self._node_exists_and_is_nonempty(dataset_node, 'raeumliche_beziehung')
        if element_text != None:
            response += u'**Räumliche Beziehung**  \n' + element_text + u'  \n'

        response += self._generate_attribute_notes(dataset_node.find('attributliste'))
        return response


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
                    'url': None, # the source url for that dataset
                    'notes': dataset_node.find('beschreibung').text,
                    'author': dataset_node.find('quelle').text,
                    'maintainer': 'Open Data Zürich',
                    'maintainer_email': 'opendata@zuerich.ch',
                    'license_id': 'to_be_filled',
                    'license_url': 'to_be_filled',
                    'tags': self._generate_tags(dataset_node),
                    'resources': self._generate_resources_dict_array(dataset + '/DEFAULT'),
                    'extras': [
                        ('spatialRelationship', self._get(dataset_node, 'raeumliche_beziehung')),
                        ('version', self._get(dataset_node, 'aktuelle_version')),
                        ('attributes', self._json_encode_attributes(self._get_attributes(dataset_node)))
                    ]
                }

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
                    log.debug('json created')

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
                # create a diff between this new metadata set and the one from yesterday.
                # send the diff to SSZ
                
                today = datetime.date.today()
                delta = datetime.timedelta(days=-1)
                yesterday = today + delta
                new_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-' + str(today))
                prev_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-' + str(yesterday))
                diff_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'diff-' + str(today))
                
                if os.path.isfile(new_metadata_path) and os.path.isfile(prev_metadata_path):
                    with open(prev_metadata_path) as prev_metadata:
                        with open(new_metadata_path) as new_metadata:
                            if prev_metadata != new_metadata:
                                # TODO check that the diff is not blank
                                with open(diff_path, 'w') as diff:
                                    d = difflib.HtmlDiff()
                                    diff.write(d.make_file(prev_metadata, new_metadata))
                    log.debug('Metadata diff generated for the dataset: ' + package_dict['id'])
                    os.remove(prev_metadata_path)
                else:
                    log.debug('Metadata JSON missing for the dataset: ' + package_dict['id'])
                
            else: # package does not exist, therefore create it.
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            # Move file around and make sure it's in the file-store
            for r in package_dict['resources']:
                if r['resource_type'] == 'file':
                    label = package_dict['datasetID'] + '/' + r['name']
                    file_contents = ''
                    with open(os.path.join(self.DROPZONE_PATH, package_dict['datasetID'], 'DEFAULT', r['name'])) as contents:
                        file_contents = contents.read()
                    params = {
                        'filename-original': 'the original file name',
                        'uploaded-by': self.config['user']
                    }
                    r['url'] = self.CKAN_SITE_URL + '/storage/f/' + label
                    self.get_ofs().put_stream(self.BUCKET, label, file_contents, params)

            if not package:
                result = self._create_or_update_package(package_dict, harvest_object)
                Session.commit()

        except Exception, e:
            log.exception(e)

        return True

    def _json_encode_attributes(self, properties):
        _dict = {}
        for key, value in properties:
            if value:
                _dict[key] = value

        return json.dumps(_dict)

    def _get_attributes(self, node):
        attribut_list = node.find('attributliste')
        attributes = []
        for attribut in attribut_list:
            attributes.append((attribut.find('sprechenderfeldname').text, attribut.find('feldbeschreibung').text))
        return attributes
