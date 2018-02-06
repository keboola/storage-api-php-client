<?php

namespace Keboola\Test\Common;

use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

class ClientRetriesTest extends StorageApiTestCase
{
    /**
     * @return callable
     */
    private static function getLinearRetryDelay()
    {
        return function (int $tries) : int {
          return $tries * 1000;
        };
    }

    public function testDefaultRetries()
    {
        $mockResponses = [
            new Response(500, [], 'This should be an error'),
            new Response(500, [], ''),
            new Response(500, [], ''),
            new Response(500, [], ''),
            new Response(200, [], 'The good response')
        ];

        $client = new Client([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 4,
            'mockResponses' => $mockResponses
        ]);

        // with 4 retries, the default exponential delay should behave like
        // first retry delay 1 sec
        // second retry delay 2 sec
        // third retry delay 4 sec
        // fourth retry delay 8 sec
        // so in 16 seconds
        $mockClient = $client->getBaseMockClient();
        $start = time();
        $response = $mockClient->request('GET', '/');
        $duration = time() - $start;

        // we'll give it +/- 1 second
        $this->assertGreaterThan(14, $duration);
        $this->assertLessThan(18, $duration);

        $this->assertEquals('The good response', $response->getBody());
    }

    public function testLinearDelay()
    {
        $mockResponses = [
            new Response(500, [], 'This should be an error'),
            new Response(500, [], ''),
            new Response(500, [], ''),
            new Response(500, [], ''),
            new Response(200, [], 'The good response')
        ];

        $client = new Client([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 4,
            'retryDelay' => self::getLinearRetryDelay(),
            'mockResponses' => $mockResponses
        ]);

        // with 4 retries, the default exponential delay should behave like
        // first retry delay 1 sec
        // second retry delay 2 sec
        // third retry delay 3 sec
        // fourth retry delay 4 sec
        // so in 9 seconds
        $mockClient = $client->getBaseMockClient();
        $start = time();
        $response = $mockClient->request('GET', '/');
        $duration = time() - $start;

        // we'll give it +/- 1 second
        $this->assertGreaterThan(7, $duration);
        $this->assertLessThan(11, $duration);

        $this->assertEquals('The good response', $response->getBody());
    }
}
