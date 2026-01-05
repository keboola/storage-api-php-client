<?php

namespace Keboola\StorageApi\Downloader;

use MicrosoftAzure\Storage\Common\Internal\Resources;
use MicrosoftAzure\Storage\Common\Internal\Validate;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddleware;

class BlobStorageRetryMiddleware
{
    //The interval will be increased linearly, the nth retry will have a
    //wait time equal to n * interval.
    public const LINEAR_INTERVAL_ACCUMULATION = 'Linear';
    //The interval will be increased exponentially, the nth retry will have a
    //wait time equal to pow(2, n) * interval.
    public const EXPONENTIAL_INTERVAL_ACCUMULATION = 'Exponential';
    //This is for the general type of logic that handles retry.
    public const GENERAL_RETRY_TYPE = 'General';
    public const DEFAULT_NUMBER_OF_RETRIES = 5;
    public const DEFAULT_RETRY_INTERVAL = Resources::DEFAULT_RETRY_INTERVAL;

    /**
     * @param int $numberOfRetries The maximum number of retries.
     * @param int $interval The minimum interval between each retry
     * @param string $accumulationMethod If the interval increases linearly or
     *                                     exponentially.
     *                                     Possible value can be
     *                                     self::LINEAR_INTERVAL_ACCUMULATION or
     *                                     self::EXPONENTIAL_INTERVAL_ACCUMULATION
     * @return RetryMiddleware             A RetryMiddleware object that contains
     *                                     the logic of how the request should be
     *                                     handled after a response.
     */
    public static function create(
        $numberOfRetries = self::DEFAULT_NUMBER_OF_RETRIES,
        $interval = self::DEFAULT_RETRY_INTERVAL,
        $accumulationMethod = self::EXPONENTIAL_INTERVAL_ACCUMULATION,
    ) {
        //Validate the input parameters
        //numberOfRetries
        Validate::isTrue(
            $numberOfRetries > 0,
            sprintf(
                Resources::INVALID_NEGATIVE_PARAM,
                'numberOfRetries',
            ),
        );
        //interval
        Validate::isTrue(
            $interval > 0,
            sprintf(
                Resources::INVALID_NEGATIVE_PARAM,
                'interval',
            ),
        );
        //accumulationMethod
        Validate::isTrue(
            $accumulationMethod === self::LINEAR_INTERVAL_ACCUMULATION ||
            $accumulationMethod === self::EXPONENTIAL_INTERVAL_ACCUMULATION,
            sprintf(
                Resources::INVALID_PARAM_GENERAL,
                'accumulationMethod',
            ),
        );

        //Get the interval calculator according to the type of the
        //accumulation method.
        $intervalCalculator =
            $accumulationMethod === self::LINEAR_INTERVAL_ACCUMULATION ?
                static::createLinearDelayCalculator($interval) :
                static::createExponentialDelayCalculator($interval);

        //Get the retry decider according to the type of the retry and
        //the number of retries.
        $retryDecider = static::createRetryDecider($numberOfRetries);

        //construct the retry middle ware.
        return new RetryMiddleware($intervalCalculator, $retryDecider);
    }

    /**
     * Create the delay calculator that increases the interval linearly
     * according to the number of retries.
     *
     * @param int $interval the minimum interval of the retry.
     *
     * @return callable      a calculator that will return the interval
     *                       according to the number of retries.
     */
    protected static function createLinearDelayCalculator($interval)
    {
        return function ($retries) use ($interval) {
            return $retries * $interval;
        };
    }

    /**
     * Create the delay calculator that increases the interval exponentially
     * according to the number of retries.
     *
     * @param int $interval the minimum interval of the retry.
     *
     * @return callable      a calculator that will return the interval
     *                       according to the number of retries.
     */
    protected static function createExponentialDelayCalculator($interval)
    {
        return function ($retries) use ($interval) {
            return $interval * (2 ** $retries);
        };
    }

    /**
     * Create the retry decider for the retry handler. It will return a callable
     * that accepts the number of retries, the request, the response and the
     * exception, and return the decision for a retry.
     *
     * @param int $maxRetries The maximum number of retries to be done.
     *
     * @return callable     The callable that will return if the request should
     *                      be retried.
     */
    protected static function createRetryDecider($maxRetries)
    {
        return function (
            $retries,
            $request,
            $response = null,
            $exception = null,
            $isSecondary = false,
        ) use (
            $maxRetries,
        ) {
            //Exceeds the retry limit. No retry.
            if ($retries >= $maxRetries) {
                return false;
            }

            if ($exception !== null) {
                return true;
            }

            if ($response === null) {
                return true;
            }

            return static::generalRetryDecider(
                $response->getStatusCode(),
                $isSecondary,
            );
        };
    }

    /**
     * Decide if the given status code indicate the request should be retried.
     *
     * @param int $statusCode Status code of the previous request.
     * @param bool $isSecondary Whether the request is sent to secondary endpoint.
     *
     * @return bool            true if the request should be retried.
     */
    protected static function generalRetryDecider($statusCode, $isSecondary)
    {
        if ($statusCode === 408) {
            return true;
        }

        if ($statusCode >= 500) {
            if ($statusCode === 501 || $statusCode === 505) {
                return false;
            }
            return true;
        }

        if ($isSecondary && $statusCode === 404) {
            return true;
        }

        return false;
    }
}
