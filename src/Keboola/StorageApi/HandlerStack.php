<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 11/11/15
 * Time: 16:09
 */
namespace Keboola\StorageApi;

use GuzzleHttp\BodySummarizer;
use GuzzleHttp\HandlerStack as HandlerStackBase;
use GuzzleHttp\Middleware;
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

    public static function create($options = [])
    {
        $handlerStack = HandlerStackBase::create($options['handler'] ?? null);

        $handlerStack->remove('http_errors');
        $handlerStack->unshift(
            Middleware::httpErrors(new BodySummarizer(self::MAX_HTTP_ERROR_MESSAGE_LENGTH)),
            'http_errors',
        );

        $handlerStack->push(Middleware::retry(
            self::createDefaultDecider(isset($options['backoffMaxTries']) ? $options['backoffMaxTries'] : 0),
            self::createExponentialDelay(),
        ));
        return $handlerStack;
    }

    private static function createDefaultDecider($maxRetries = 3)
    {
        return function (
            $retries,
            RequestInterface $request,
            ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            // don't do retry if server returns 501 not implemented
            if ($response && $response->getStatusCode() === 501) {
                return false;
            }

            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() > 499) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }

    private static function createExponentialDelay()
    {
        return function ($retries) {
            return (int) pow(2, $retries - 1) * 1000;
        };
    }
}
