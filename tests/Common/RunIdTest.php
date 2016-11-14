<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Event;

class RunIdTest extends StorageApiTestCase
{


    public function testRunIdCreate()
    {
        $runId = $this->_client->generateRunId();
        $this->assertNotEmpty($runId);
    }

    public function testRunIdCreateFromPrevious()
    {
        $previousRunId = '234234';

        $runId = $this->_client->generateRunId($previousRunId);
        $this->assertNotEmpty($runId);
        $this->assertStringStartsWith("$previousRunId.", $runId);
    }

    public function testRunIdFiltering()
    {
        $topLevelRunId = $this->_client->generateRunId();

        $this->createEvent($topLevelRunId);

        $secondLevelRunId1 = $this->_client->generateRunId($topLevelRunId);
        $this->createEvent($secondLevelRunId1);
        $this->createEvent($secondLevelRunId1);

        $secondLevelRunId2 = $this->_client->generateRunId($topLevelRunId);
        $this->createEvent($secondLevelRunId2);
        $this->createEvent($secondLevelRunId2);
        $this->createEvent($secondLevelRunId2);

        $events = $this->_client->listEvents(array(
            'runId' => $topLevelRunId,
        ));
        $this->assertCount(6, $events);

        $events = $this->_client->listEvents(array(
            'runId' => $secondLevelRunId1,
        ));
        $this->assertCount(2, $events);

        $events = $this->_client->listEvents(array(
            'runId' => $secondLevelRunId2,
        ));
        $this->assertCount(3, $events);
    }

    /**
     * @param $runId
     * @return int id
     */
    private function createEvent($runId)
    {
        $event = new Event();
        $event
            ->setComponent('transformation')
            ->setRunId($runId)
            ->setType('info')
            ->setMessage('test');
        return $this->createAndWaitForEvent($event);
    }
}
