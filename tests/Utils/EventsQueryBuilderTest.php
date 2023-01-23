<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Generator;
use PHPUnit\Framework\TestCase;

class EventsQueryBuilderTest extends TestCase
{
    /**
     * @dataProvider eventsQueryBuilderProvider
     */
    public function testBuildQuery(EventsQueryBuilder $queryBuilder, $expectedQuery)
    {
        $this->assertSame($expectedQuery, $queryBuilder->generateQuery());
    }

    public function eventsQueryBuilderProvider(): Generator
    {
        yield 'tokenId and runId' => [
            (new EventsQueryBuilder())->setTokenId('1234-1234')->setRunId('123'),
            'token.id:1234-1234 AND runId:123'
        ];

        yield 'one parameter' => [
            (new EventsQueryBuilder())->setRunId('123'),
            'runId:123'
        ];

        yield 'all parameters' => [
            (new EventsQueryBuilder())
                ->setTokenId('123-token')
                ->setEvent('storage.createBucket')
                ->setObjectId('obj-123')
                ->setIdBranch('branch-123')
                ->setComponent('wr-db')
                ->setRunId('123')
                ->setProjectId('project-123')
                ->setObjectType('obj-123'),
            'token.id:123-token AND event:storage.createBucket AND objectId:obj-123 AND idBranch:branch-123'
            . ' AND component:wr-db AND runId:123 AND project.id:project-123 AND objectType:obj-123'
        ];

        yield 'other random params' => [
            (new EventsQueryBuilder())->setProjectId('1234-1234')->setEvent('123'),
            'project.id:1234-1234 AND event:123'
        ];
    }
}
