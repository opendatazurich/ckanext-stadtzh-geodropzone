#coding: utf-8

import os
from lxml import etree

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json

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

    config = {
        'user': u'harvest'
    }

    DROPZONE_PATH = u'/vagrant/vagrant/templates/default/GEO'


    # def _guess_format(self, file_name):
    #     '''
    #     Return the format for a given full filename
    #     '''
    #     _, file_extension = os.path.splitext(file_name.lower())
    #     return file_extension[1:]

    # def _generate_resources_dict_array(self, dataset_id):
    #     '''

    #     '''
    #     try:
    #         resources = []
    #         prefix = self.DEPARTMENT_BASE + dataset_id + u'/'
    #         bucket_list = self._get_s3_bucket().list(prefix=prefix)
    #         for file in bucket_list:
    #             if file.key != prefix:
    #                 resources.append({
    #                     'url': self.FILES_BASE_URL + '/' + file.key,
    #                     'name': file.key.replace(prefix, u''),
    #                     'format': self._guess_format(file.key)
    #                     })
    #         return resources
    #     except Exception, e:
    #         log.exception(e)
    #         return []


    # def _get_row_dict_array(self, lang_index):
    #     '''
    #     '''
    #     try:
    #         metadata_workbook = xlrd.open_workbook(self.METADATA_FILE_NAME)
    #         worksheet = metadata_workbook.sheet_by_index(lang_index)

    #         # Extract the row headers
    #         header_row = worksheet.row_values(6)
    #         rows = []
    #         for row_num in range(worksheet.nrows):
    #             # Data columns begin at row count 7 (8 in Excel)
    #             if row_num >= 7:
    #                 rows.append(dict(zip(header_row, worksheet.row_values(row_num))))
    #         return rows

    #     except Exception, e:
    #         log.exception(e)
    #         return []


    # def _generate_term_translations(self, lang_index):
    #     '''
    #     '''
    #     try:
    #         translations = []

    #         de_rows = self._get_row_dict_array(0)
    #         other_rows = self._get_row_dict_array(lang_index)

    #         log.debug(de_rows)
    #         log.debug(other_rows)

    #         keys = ['title', 'notes', 'author', 'maintainer', 'licence']

    #         for row_idx in range(len(de_rows)):
    #             for key in keys:
    #                 translations.append({
    #                     'lang_code': self.LANG_CODES[lang_index],
    #                     'term': de_rows[row_idx][key],
    #                     'term_translation': other_rows[row_idx][key]
    #                     })

    #             de_tags = de_rows[row_idx]['tags'].split(u', ')
    #             other_tags = other_rows[row_idx]['tags'].split(u', ')

    #             if len(de_tags) == len(other_tags):
    #                 for tag_idx in range(len(de_tags)):
    #                     translations.append({
    #                         'lang_code': self.LANG_CODES[lang_index],
    #                         'term': de_tags[tag_idx],
    #                         'term_translation': other_tags[tag_idx]
    #                         })

    #         return translations


    #     except Exception, e:
    #         log.exception(e)
    #         return []

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
        resource_files = self._remove_hidden_files(os.listdir(os.path.join(self.DROPZONE_PATH, dataset)))
        log.debug(resource_files)

        for resource_file in resource_files:
            if resource_file == u'meta.xml':
                break
            elif resource_file == u'link.xml':
                with open(os.path.join(self.DROPZONE_PATH, dataset, resource_file), 'r') as links_xml:
                    parser = etree.XMLParser(encoding='utf-8')
                    links = etree.fromstring(links_xml.read(), parser=parser).findall('link')
                    for link in links:
                        resources.append({
                            'url': link.find('url').text,
                            'name': link.find('lable').text,
                            'format': link.find('type').text,
                            'resource_type': 'api'
                        })
            else:
                resources.append({
                    'url': 'http://example.org/' + resource_file,
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

    def _generate_notes(self, dataset_node):
        '''
        Compose the notes given the elements available within the node
        '''
        response = u''
        if dataset_node.find('beschreibung').text != None:
            response += u'**Details**  \n' + dataset_node.find('beschreibung').text + u'  \n'
        response += u'**Urheber**  \n' + u'  \n'
        response += u'**Erstmalige Veröffentlichung**  \n' + u'  \n'
        if dataset_node.find('zeitraum').text != None:
            response += u'**Zeitraum**  \n' + dataset_node.find('zeitraum').text + u'  \n'
        response += u'**Aktualisierungsintervall**  \n' + u'  \n'
        if dataset_node.find('aktuelle_version').text != None:
            response += u'**Aktuelle Version**  \n' + dataset_node.find('aktuelle_version').text + u'  \n'
        response += u'**Aktualisierungsdatum**  \n' + 'insert the current date here' + u'  \n'
        response += u'**Datentyp**  \n' + u'  \n'
        if dataset_node.find('quelle').text != None:
            response += u'**Quelle**  \n' + dataset_node.find('quelle').text + u'  \n'
        if dataset_node.find('raeumliche_beziehung').text != None:
            response += u'**Räumliche Beziehung**  \n' + dataset_node.find('raeumliche_beziehung').text + u'  \n'

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
                    # 'notes': dataset_node.find('beschreibung').text,
                    'notes': self._generate_notes(dataset_node),
                    'author': dataset_node.find('quelle').text,
                    'maintainer': 'Open Data Zürich',
                    'maintainer_email': 'opendata@zuerich.ch',
                    'license_id': 'to_be_filled',
                    'license_url': 'to_be_filled',
                    'tags': self._generate_tags(dataset_node),
                    'resources': self._generate_resources_dict_array(dataset + '/DEFAULT'),
                }

                obj = HarvestObject(
                    guid = metadata['datasetID'],
                    job = harvest_job,
                    content = json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + metadata['datasetID'] + ' to the queue')
                ids.append(obj.id)

        return ids

        # parser = etree.XMLParser(encoding='utf-8')
        # for package in etree.fromstring(metadata_file.data, parser=parser):

        #     # Get the german dataset if one is available, otherwise get the first one
        #     base_datasets = package.xpath("dataset[@xml:lang='de']")
        #     if len(base_datasets) != 0:
        #         base_dataset = base_datasets[0]
        #     else:
        #         base_dataset = package.find('dataset')

        #     metadata = self._generate_metadata(base_dataset, package)
        #     if metadata:
        #         obj = HarvestObject(
        #             guid = base_dataset.get('datasetID'),
        #             job = harvest_job,
        #             content = json.dumps(metadata)
        #         )
        #         obj.save()
        #         log.debug('adding ' + base_dataset.get('datasetID') + ' to the queue')
        #         ids.append(obj.id)
        #     else:
        #         log.debug('Skipping ' + base_dataset.get('datasetID') + ' since no resources are available')



        # self._fetch_metadata_file()
        # ids = []

        # de_rows = self._get_row_dict_array(0)
        # for row in de_rows:
        #     # Construct the metadata dict for the dataset on CKAN
        #     metadata = {
        #         'datasetID': row[u'id'],
        #         'title': row[u'title'],
        #         'notes': row[u'notes'],
        #         'author': row[u'author'],
        #         'maintainer': row[u'maintainer'],
        #         'maintainer_email': row[u'maintainer_email'],
        #         'license_id': row[u'licence'],
        #         'translations': [],
        #         'tags': row[u'tags'].split(u', '),
        #         'groups': []
        #     }

        #     metadata['resources'] = self._generate_resources_dict_array(row[u'id'])
        #     log.debug(metadata['resources'])

        #     # Adding term translations
        #     metadata['translations'].extend(self._generate_term_translations(1)) # fr
        #     metadata['translations'].extend(self._generate_term_translations(2)) # it
        #     metadata['translations'].extend(self._generate_term_translations(3)) # en

        #     log.debug(metadata['translations'])

        #     obj = HarvestObject(
        #         guid = row[u'id'],
        #         job = harvest_job,
        #         content = json.dumps(metadata)
        #     )
        #     obj.save()
        #     log.debug('adding ' + row[u'id'] + ' to the queue')
        #     ids.append(obj.id)

        #     log.debug(de_rows)

        # return ids


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
            package_dict['name'] = self._gen_new_name(package_dict[u'datasetID'])

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
                    'id': self._gen_new_name(self.ORGANIZATION['de']),
                    'name': self._gen_new_name(self.ORGANIZATION['de']),
                    'title': self.ORGANIZATION['de']
                }
                package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
            except:
                organization = get_action('organization_create')(context, data_dict)
                package_dict['owner_org'] = organization['id']

            # Insert or update the package
            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            result = self._create_or_update_package(package_dict, harvest_object)
            Session.commit()

        except Exception, e:
            log.exception(e)

        return True
