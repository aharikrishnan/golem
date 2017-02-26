# encoding: utf-8
class EasyJsonSerializer
  attr_reader :value

  def initialize val
    self.value = val
  end

  class << self
  def load(value)
    JSON.load(value)
  end

  def dump(value)
    value.to_json
  end
  end
end
