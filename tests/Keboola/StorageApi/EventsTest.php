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
		$event->setComponent('ex-sfdc')
			->setConfigurationId('sys.c-sfdc.account-01')
			->setDuration(200)
			->setType('info')
			->setRunId('ddddssss')
			->setMessage('Table Opportunity fetched.')
			->setDescription('Some longer description of event')
			->setParams(array(
				'accountName' => 'Keboola',
				'configuration' => 'sys.c-sfdc.sfdc-01',
			));

		$id = $this->_client->createEvent($event);
		$savedEvent = $this->_client->getEvent($id);

		$this->assertEquals($event->getComponent(), $savedEvent['component']);
		$this->assertEquals($event->getConfigurationId(), $savedEvent['configurationId']);
		$this->assertEquals($event->getMessage(), $savedEvent['message']);
		$this->assertEquals($event->getDescription(), $savedEvent['description']);
		$this->assertEquals($event->getType(), $savedEvent['type']);
	}

	/**
	 * @expectedException Keboola\StorageApi\Exception
	 */
	public function testInvalidType()
	{
		$event = new Event();
		$event->setType('homeless');
	}

	public function testEventCompatibility()
	{
		$event = new Event();
		$event->setComponentName('sys.c-sfdc.account-01')
			->setComponentType('ex-sfdc')
			->setMessage('test');


		$id = $this->_client->createEvent($event);
		$savedEvent = $this->_client->getEvent($id);
		$this->assertEquals($event->getComponentName(), $savedEvent['configurationId']);
		$this->assertEquals($event->getComponentType(), $savedEvent['component']);
	}

	public function testEventList()
	{
		// at least one event should be generated
		$this->_client->listBuckets();

		$events = $this->_client->listEvents(1);
		$this->assertCount(1, $events);
	}

	public function testRunIdPropagation()
	{
		$runId = 'some-run-id';
		$this->_client->setRunId($runId);

		// generate event
		$this->_client->listBuckets();

		$events = $this->_client->listEvents(1);
		$event = reset($events);

		$this->assertEquals($runId, $event['runId']);
	}

}