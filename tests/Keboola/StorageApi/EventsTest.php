<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

use Keboola\StorageApi\Event;

class Keboola_StorageApi_EventsTest extends StorageApiTestCase
{

	public function testEventCreate()
	{
		$event = new Event();
		$event->setComponentName('SFDC')
			->setComponentType('Extractor')
			->setDuration(200)
			->setMessage('Table Opportunity fetched.')
			->setParams(array(
				'accountName' => 'Keboola',
				'configuration' => 'sys.c-sfdc.sfdc-01',
			));

		$id = $this->_client->createEvent($event);
		$this->assertGreaterThan(0, $id);
	}

}