<?php
/**
 *
 * User: Martin Halamíček
 * Date: 30.11.12
 * Time: 16:12
 *
 */

namespace Keboola\StorageApi;


class Event {


	const TYPE_INFO = 'info';
	const TYPE_SUCCESS = 'success';
	const TYPE_WARN = 'warn';
	const TYPE_ERROR = 'error';

	/**
	 * @var
	 */
	private $_component;

	/**
	 * @var
	 */
	private $_configurationId;

	/**
	 * @var
	 */
	private $_runId;

	/**
	 * Log message
	 * @var string
	 */
	private $_message;

	/**
	 * More detailed description
	 * @var
	 */
	private $_description;

	/**
	 * info | error
	 * @var string
	 */
	private $_type;

	/**
	 * Params associated to event e.q. sfdc configuration id
	 * @var array
	 */
	private $_params = array();

	/**
	 * Results associated to event, some performance metrics, fetched rows. couts etc.
	 * @var array
	 */
	private $_results = array();

	/**
	 * @var int run duration in seconds
	 */
	private $_duration;

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
		return $this->_message;
	}

	/**
	 * @param $message
	 * @return Event
	 */
	public function setMessage($message)
	{
		$this->_message = $message;
		return $this;
	}

	public function getType()
	{
		return $this->_type;
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

		$this->_type = $type;
		return $this;
	}

	public function getParams()
	{
		return $this->_params;
	}

	/**
	 * @param array $params
	 * @return Event
	 */
	public function setParams(array $params)
	{
		$this->_params = $params;
		return $this;
	}

	public function getResults()
	{
		return $this->_results;
	}

	/**
	 * @param $results
	 * @return Event
	 */
	public function setResults(array $results)
	{
		$this->_results = $results;
		return $this;
	}

	public function getDuration()
	{
		return $this->_duration;
	}

	/**
	 * @param $duration
	 * @return Event
	 */
	public function setDuration($duration)
	{
		$this->_duration = (int) $duration;
		return $this;
	}

	public function getComponent()
	{
		return $this->_component;
	}

	/**
	 * @param $component
	 * @return Event
	 */
	public function setComponent($component)
	{
		$this->_component = $component;
		return $this;
	}

	/**
	 * @return mixed
	 */
	public function getConfigurationId()
	{
		return $this->_configurationId;
	}

	/**
	 * @param $configurationId
	 * @return Event
	 */
	public function setConfigurationId($configurationId)
	{
		$this->_configurationId = $configurationId;
		return $this;
	}

	/**
	 * @return
	 */
	public function getRunId()
	{
		return $this->_runId;
	}

	/**
	 * @param $runId
	 * @return Event
	 */
	public function setRunId($runId)
	{
		$this->_runId = $runId;
		return $this;
	}

	/**
	 * @return
	 */
	public function getDescription()
	{
		return $this->_description;
	}


	/**
	 * @param $description
	 * @return Event
	 */
	public function setDescription($description)
	{
		$this->_description = $description;
		return $this;
	}

}
