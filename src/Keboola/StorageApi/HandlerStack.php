<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 11/11/15
 * Time: 16:09
 */
namespace Keboola\StorageApi;

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
    public static function create($options = [])
    {
        $handlerStack = HandlerStackBase::create();
        $handlerStack->push(Middleware::retry(
            self::createDefaultDecider(isset($options['backoffMaxTries']) ? $options['backoffMaxTries'] : 0),
            self::createExponentialDelay()
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
            return (int)pow(2, $retries - 1) * 1000;
        };
    }
}
