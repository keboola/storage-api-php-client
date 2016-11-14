<?php
namespace Keboola\StorageApi;

class Exception extends \Exception
{
    /**
     * @var null|\Exception
     */
    private $previous = null;

    protected $stringCode;

    protected $contextParams;

    /**
     * Construct the exception
     *
     * @param null $message
     * @param  int $code
     * @param  \Exception $previous
     * @param  string $stringCode
     * @param  mixed|array $params
     * @return \Keboola\StorageApi\Exception
     */
    public function __construct($message = null, $code = null, $previous = null, $stringCode = null, $params = null)
    {
        $this->setStringCode($stringCode);
        $this->setContextParams($params);
        if (version_compare(PHP_VERSION, '5.3.0', '<')) {
            parent::__construct($message, (int)$code);
            $this->previous = $previous;
        } else {
            parent::__construct($message, (int)$code, $previous);
        }
    }


    public function getStringCode()
    {
        return $this->stringCode;
    }

    /**
     * @param $stringCode
     * @return Exception
     */
    public function setStringCode($stringCode)
    {
        if ($stringCode) {
            $this->stringCode = (string)$stringCode;
        } else {
            $this->stringCode = "APPLICATION_ERROR";
        }
        return $this;
    }

    public function getContextParams()
    {
        return $this->contextParams;
    }

    /**
     * @param array $contextParams
     * @return Exception
     */
    public function setContextParams($contextParams)
    {
        $this->contextParams = (array)$contextParams;
        return $this;
    }
}
