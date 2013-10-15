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


    def info(self):
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
        datasets = os.listdir(self.DROPZONE_PATH)

        # foreach -> meta.xml -> create entry
        for dataset in datasets:
            with open(os.path.join(self.DROPZONE_PATH, dataset, 'DEFAULT/meta.xml'), 'r') as meta_xml:
                parser = etree.XMLParser(encoding='utf-8')
                # for data_collection in etree.fromstring(meta_xml.read(), parser=parser).find('datensammlung'):

                log.debug(dataset)

                contents = meta_xml.read()
                log.debug(contents)

                data_collection = etree.fromstring(contents, parser=parser)
                log.debug(data_collection)

                log.debug(data_collection.find('datensatz').text)

                metadata = {
                    'datasetID': dataset,
                    'title': data_collection.find('datensatz').find('titel').text,
                    'notes': data_collection.find('datensatz').find('beschreibung').text,
                    'author': 'foobar',
                    'maintainer': 'hagsdkfjhag',
                    'maintainer_email': 'jahdfk@jsdgfj.cs',
                    'license_id': 'ahdfgkajshdf',
                    'tags': [],
                    'groups': []
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

                # log.debug(de_rows)
        


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
