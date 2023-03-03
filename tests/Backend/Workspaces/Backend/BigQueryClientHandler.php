<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Workspaces\Backend;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

class BigQueryClientHandler
{
    private Client $client;
    private int $maxRetries;

    public function __construct(Client $client, int $maxRetries = 10)
    {
        $this->client = $client;
        $this->maxRetries = $maxRetries;
    }

    public function __invoke(RequestInterface $request, array $options): ResponseInterface
    {
        $retries = 0;
        do {
            try {
                return $this->client->send($request);
            } catch (RequestException $e) {
                if ($retries >= $this->maxRetries) {
                    throw $e;
                }
                echo sprintf('Retrying (%s/%s)' . PHP_EOL, $retries + 1, $this->maxRetries);
            }
            usleep(500000); // wait for 0.5 seconds before retrying
            $retries++;
        } while ($retries <= $this->maxRetries);
        throw new \LogicException('Max retries exceeded');
    }
}
