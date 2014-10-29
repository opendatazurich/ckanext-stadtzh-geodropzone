# coding: utf-8

from ckanext.stadtzhharvest.harvester import StadtzhHarvester
import logging

log = logging.getLogger(__name__)


class StadtzhgeodropzoneHarvester(StadtzhHarvester):
    '''
    The harvester for the Stadt ZH GEO Dropzone
    '''

    DATA_PATH = '/usr/lib/ckan/GEO'
    META_DIR = 'DEFAULT'
    METADATA_DIR = 'geo-metadata'

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
        return self._fetch_datasets(harvest_object)

    def import_stage(self, harvest_object):
        log.debug('In StadtzhgeodropzoneHarvester import_stage')
        return self._import_datasets(harvest_object)
