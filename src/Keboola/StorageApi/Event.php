<?php
/**
 *
 * User: Martin Halamíček
 * Date: 30.11.12
 * Time: 16:12
 *
 */

namespace Keboola\StorageApi;

class Event
{


    const TYPE_INFO = 'info';
    const TYPE_SUCCESS = 'success';
    const TYPE_WARN = 'warn';
    const TYPE_ERROR = 'error';

    /**
     * @var
     */
    private $component;

    /**
     * @var
     */
    private $configurationId;

    /**
     * @var
     */
    private $runId;

    /**
     * Log message
     * @var string
     */
    private $message;

    /**
     * More detailed description
     * @var
     */
    private $description;

    /**
     * info | error
     * @var string
     */
    private $type;

    /**
     * Params associated to event e.q. sfdc configuration id
     * @var array
     */
    private $params = array();

    /**
     * Results associated to event, some performance metrics, fetched rows. couts etc.
     * @var array
     */
    private $results = array();

    /**
     * @var int run duration in seconds
     */
    private $duration;

    /**
     * @deprecated
     * @return mixed
     */
    public function getComponentName()
    {
        return $this->getConfigurationId();
    }

    /**
     * @deprecated
     * @param $componentName
     * @return Event
     */
    public function setComponentName($componentName)
    {
        return $this->setConfigurationId($componentName);
    }

    /**
     * @deprecated
     * @return mixed
     */
    public function getComponentType()
    {
        return $this->getComponent();
    }

    /**
     * @deprecated
     * @param $componentType
     * @return Event
     */
    public function setComponentType($componentType)
    {
        return $this->setComponent($componentType);
    }

    public function getMessage()
    {
        return $this->message;
    }

    /**
     * @param $message
     * @return Event
     */
    public function setMessage($message)
    {
        $this->message = $message;
        return $this;
    }

    public function getType()
    {
        return $this->type;
    }

    /**
     * @param $type
     * @throws Exception
     * @return Event
     */
    public function setType($type)
    {
        $allowedTypes = array(
            self::TYPE_ERROR,
            self::TYPE_INFO,
            self::TYPE_SUCCESS,
            self::TYPE_WARN
        );
        if (!in_array($type, $allowedTypes)) {
            throw new Exception("{$type} is not allowed. Allowed types: " . implode(',', $allowedTypes));
        }

        $this->type = $type;
        return $this;
    }

    public function getParams()
    {
        return $this->params;
    }

    /**
     * @param array $params
     * @return Event
     */
    public function setParams(array $params)
    {
        $this->params = $params;
        return $this;
    }

    public function getResults()
    {
        return $this->results;
    }

    /**
     * @param $results
     * @return Event
     */
    public function setResults(array $results)
    {
        $this->results = $results;
        return $this;
    }

    public function getDuration()
    {
        return $this->duration;
    }

    /**
     * @param $duration
     * @return Event
     */
    public function setDuration($duration)
    {
        $this->duration = (int)$duration;
        return $this;
    }

    public function getComponent()
    {
        return $this->component;
    }

    /**
     * @param $component
     * @return Event
     */
    public function setComponent($component)
    {
        $this->component = $component;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getConfigurationId()
    {
        return $this->configurationId;
    }

    /**
     * @param $configurationId
     * @return Event
     */
    public function setConfigurationId($configurationId)
    {
        $this->configurationId = $configurationId;
        return $this;
    }

    /**
     * @return
     */
    public function getRunId()
    {
        return $this->runId;
    }

    /**
     * @param $runId
     * @return Event
     */
    public function setRunId($runId)
    {
        $this->runId = $runId;
        return $this;
    }

    /**
     * @return
     */
    public function getDescription()
    {
        return $this->description;
    }


    /**
     * @param $description
     * @return Event
     */
    public function setDescription($description)
    {
        $this->description = $description;
        return $this;
    }
}
