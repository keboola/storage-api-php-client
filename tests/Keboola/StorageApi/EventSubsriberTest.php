<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 26/06/14
 * Time: 11:38
 * To change this template use File | Settings | File Templates.
 */

class Keboola_StorageApi_EventSubsriberTest_SimpleSubsriber implements \GuzzleHttp\Event\SubscriberInterface
{

	/**
	 * Returns an array of event names this subscriber wants to listen to.
	 *
	 * The returned array keys MUST map to an event name. Each array value
	 * MUST be an array in which the first element is the name of a function
	 * on the EventSubscriber. The second element in the array is optional, and
	 * if specified, designates the event priority.
	 *
	 * For example:
	 *
	 *  - ['eventName' => ['methodName']]
	 *  - ['eventName' => ['methodName', $priority]]
	 *
	 * @return array
	 */
	public function getEvents()
	{
		return [
			'before'   => ['onBefore'],
		];
	}

	public function onBefore(\GuzzleHttp\Event\BeforeEvent $event)
	{
		echo 'Before!';
	}

}

class Keboola_StorageApi_EventSubsriberTest extends StorageApiTestCase
{


	public function testSubscriber()
	{
		$subscriber = $this->getMockBuilder('Keboola_StorageApi_EventSubsriberTest_SimpleSubsriber')
			->setMethods(array('onBefore'))
			->getMock();

		$subscriber->expects($this->once())
			->method('onBefore');

		$client = new \Keboola\StorageApi\Client(array(
			'token' => STORAGE_API_TOKEN,
			'url' => STORAGE_API_URL,
			'eventSubscriber' => $subscriber,
		));
		$client->verifyToken();
	}

}