<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\ExpectationFailedException;
use PHPUnit\Framework\TestCase;

class EventTesterUtilsTest extends TestCase
{
    /** @var EventTesterUtils */
    private $sut;

    public function setUp(): void
    {
        $this->sut = new class() {
            use EventTesterUtils;

            public function __construct(
            )
            {
                $this->tokenId = 7;
                $this->lastEventId = 10;
            }
        };
    }

    public function testWillRetryWhenEventsNotThere(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::exactly(3))->method('listEvents')->willReturn(['a' ], ['a' ], ['a', 'b', 'c' ]);
        $this->sut->assertEventsCallback($clientMock, function ($events) {
            $this->assertCount(3, $events);
        });
    }

    public function testWillFailWhenRetriesAreSpent(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::exactly(20))->method('listEvents')->willReturn(['a' ]);
        try {
            $this->sut->assertEventsCallback($clientMock, function ($events) {
                $this->assertCount(4, $events);
            });
            $this->fail('Should have failed');
        } catch (ExpectationFailedException $e) {
            $this->assertSame('Failed asserting that actual size 1 matches expected size 4.', $e->getMessage());
        }
    }
}
