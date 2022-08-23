<?php
/**
 *
 * Test if an error message from API raises a ClientException
 *
 * User: Ondrej Hlavacek
 * Date: 11.12.12
 * Time: 17:22 PST
 *
 */
namespace Keboola\Test\Common;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class ExceptionsTest extends StorageApiTestCase
{
    public function testLongErrorResponseIsNotTruncated(): void
    {
        $responseBody = str_repeat('X', 1024*1024);

        $mockHandler = new MockHandler([
            new Response(400, [], $responseBody),
        ]);

        $client = $this->getClient([
            'token' => 'foo',
            'url' => 'http://example.com',
            'handler' => $mockHandler,
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($responseBody);

        $client->apiGet('/');
    }

    public function testException(): void
    {
        $this->expectException(\Keboola\StorageApi\ClientException::class);
        $t = $this->_client->getTable('nonexistingtable');
    }
}
