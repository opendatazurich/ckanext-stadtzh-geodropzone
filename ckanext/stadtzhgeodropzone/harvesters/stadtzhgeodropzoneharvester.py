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
            self._import_package(harvest_object)
            Session.commit()

        except Exception, e:
            log.exception(e)

        return True
