<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Event;
use PHPUnit\Framework\ExpectationFailedException;
use PHPUnit\Framework\TestCase;

class EventTesterUtilsTest extends TestCase
{
    private EventTesterUtilsImpl $sut;

    public function setUp(): void
    {
        $this->sut = new EventTesterUtilsImpl();
    }

    public function testWillRetryWhenEventsNotThere(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::exactly(3))->method('listEvents')->willReturn(['a'], ['a'], ['a', 'b', 'c']);
        $this->sut->assertEventsCallback($clientMock, function ($events) {
            $this->assertCount(3, $events);
        });
    }

    public function testWillFailWhenRetriesAreSpent(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::exactly(20))->method('listEvents')->willReturn(['a']);
        try {
            $this->sut->assertEventsCallback($clientMock, function ($events) {
                $this->assertCount(4, $events);
            });
            $this->fail('Should have failed');
        } catch (ExpectationFailedException $e) {
            $this->assertSame('Failed asserting that actual size 1 matches expected size 4.', $e->getMessage());
        }
    }

    public function testCreateAndWaitForEvent(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())->method('createEvent')->willReturn(1);
        $event = new Event();
        $clientMock->expects(self::once())->method('getEvent')->with(1)->willReturn(['something' => true]);

        $res = $this->sut->createAndWaitForEvent($event, $clientMock);
        $this->assertSame(['something' => true], $res);
    }

    public function testCreateAndWaitForEventNoEvent(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())->method('createEvent')->willReturn(1);
        $event = new Event();
        $clientMock->expects(self::exactly(20))->method('getEvent')->with(1)->willThrowException(new ClientException());

        $this->expectException(ClientException::class);
        $this->sut->createAndWaitForEvent($event, $clientMock);
    }
}
