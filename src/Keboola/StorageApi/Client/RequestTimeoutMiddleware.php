<?php

namespace Keboola\StorageApi\Client;

use Keboola\StorageApi\Client;
use Psr\Http\Message\RequestInterface;

class RequestTimeoutMiddleware
{
    public const REQUEST_TIMEOUT_DEFAULT = 120;
    public const REQUEST_TIMEOUT_EXTENDED = 7200;

    /** @var callable */
    private $nextHandler;

    private function __construct(
        callable $nextHandler,
    ) {
        $this->nextHandler = $nextHandler;
    }

    /**
     * @return callable
     */
    public static function factory()
    {
        return function (callable $handler) {
            return new self($handler);
        };
    }

    /**
     * @return mixed
     */
    public function __invoke(
        RequestInterface $request,
        array $options,
    ) {
        $nextHander = $this->nextHandler;

        $isExtendedTimeout = array_key_exists(Client::REQUEST_OPTION_EXTENDED_TIMEOUT, $options) ? (bool) $options[Client::REQUEST_OPTION_EXTENDED_TIMEOUT] : false;
        if ($request->getMethod() === 'DELETE') {
            $isExtendedTimeout = true;
        }

        $options['timeout'] = self::REQUEST_TIMEOUT_DEFAULT;
        if ($isExtendedTimeout) {
            $options['timeout'] = self::REQUEST_TIMEOUT_EXTENDED;
        }
        return $nextHander($request, $options);
    }
}
