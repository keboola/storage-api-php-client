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

		$id = $this->createAndWaitForEvent($event);
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


		$id = $this->createAndWaitForEvent($event);
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

	public function testEventsFiltering()
	{
		$events = $this->_client->listEvents(array(
			'limit' => 100,
			'offset' => 0
		));

		$lastEvent = reset($events);
		$lastEventId = $lastEvent['id'];

		$runId = 'test';
		$event = new Event();
		$event
			->setComponent('transformation')
			->setRunId($runId)
			->setType('info')
			->setMessage('test')
			->setConfigurationId('myConfig');
		$this->createAndWaitForEvent($event);

		$event->setComponent('ex-fb');
		$this->createAndWaitForEvent($event);
		$event->setMessage('another');
		$this->createAndWaitForEvent($event);
		$events = $this->_client->listEvents(array(
			'sinceId' => $lastEventId,
		));

		$this->assertCount(3, $events);

		$events = $this->_client->listEvents(array(
			'sinceId' => $lastEventId,
			'component' => 'transformation',
		));
		$this->assertCount(1, $events, 'filter by component');

		$event->setRunId('rundId2');
		$this->createAndWaitForEvent($event);

		$events = $this->_client->listEvents(array(
			'sinceId' => $lastEventId,
			'runId' => $runId,
		));
		$this->assertCount(3, $events);
	}

	protected function createAndWaitForEvent(Event $event)
	{
		$id = $this->_client->createEvent($event);

		$tries = 0;
		while (true) {
			try {
				$this->_client->getEvent($id);
				return $id;
			} catch(\Keboola\StorageApi\ClientException $e) {}
			if ($tries > 4) {
				throw new \Exception('Max tries exceeded.');
			}
			$tries++;
			sleep(pow(2, $tries));
		}

	}
}