<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Client;

use GuzzleHttp\MessageFormatterInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class LogMessageFormatter implements MessageFormatterInterface
{
    public function format(
        RequestInterface $request,
        ?ResponseInterface $response = null,
        ?Throwable $error = null
    ): string {
        $message =
            $request->getMethod() . ' ' .
            $request->getUri() . ' ' .
            ($response ? $response->getStatusCode() : 'NULL')
        ;

        if ($error !== null) {
            // json_encode is a simple way to make the response single-line
            $encodedResponse = $response ? json_encode((string) $response->getBody()) : 'NULL';

            $message .= ' ' . $encodedResponse;
        }

        return $message;
    }
}
