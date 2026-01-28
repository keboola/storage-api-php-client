<?php
namespace Keboola\StorageApi;

use GuzzleHttp\BodySummarizer;
use GuzzleHttp\HandlerStack as HandlerStackBase;
use GuzzleHttp\Middleware;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * @private
 * Class HandlerStack
 * @package Keboola\StorageApi
 */
final class HandlerStack
{
    private const MAX_HTTP_ERROR_MESSAGE_LENGTH = 1024*1024;

    /**
     * @param array{
     *     handler?: callable(RequestInterface, array): PromiseInterface,
     *     backoffMaxTries?: int,
     *     retryOnMaintenance?: bool,
     * } $options
     */
    public static function create(array $options = []): HandlerStackBase
    {
        $handlerStack = HandlerStackBase::create($options['handler'] ?? null);

        $handlerStack->remove('http_errors');
        $handlerStack->unshift(
            Middleware::httpErrors(new BodySummarizer(self::MAX_HTTP_ERROR_MESSAGE_LENGTH)),
            'http_errors',
        );

        $handlerStack->push(Middleware::retry(
            self::createDefaultDecider(
                $options['backoffMaxTries'] ?? 0,
                $options['retryOnMaintenance'] ?? true,
            ),
            self::createExponentialDelay(),
        ));
        return $handlerStack;
    }

    private static function createDefaultDecider(int $maxRetries, bool $retryOnMaintenance): callable
    {
        return function (
            $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $error = null,
        ) use (
            $maxRetries,
            $retryOnMaintenance,
        ) {
            // don't do retry if server returns 501 not implemented
            if ($response && $response->getStatusCode() === 501) {
                return false;
            }

            if ($response && $response->getStatusCode() === 503 && !$retryOnMaintenance) {
                return false;
            }

            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() === 409) {
                // Retry on 409 Conflict if it's a version conflict (deadlock) error
                return self::isVersionConflictResponse($response);
            } elseif ($response && $response->getStatusCode() > 499) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }

    private static function isVersionConflictResponse(ResponseInterface $response): bool
    {
        $body = (string) $response->getBody();
        if ($response->getBody()->isSeekable()) {
            $response->getBody()->rewind();
        }

        $data = json_decode($body, true);
        if (!is_array($data)) {
            return false;
        }

        return isset($data['code']) && $data['code'] === 'storage.components.configurations.versionConflict';
    }

    private static function createExponentialDelay(): callable
    {
        return function ($retries) {
            return (int) pow(2, $retries - 1) * 1000;
        };
    }
}
