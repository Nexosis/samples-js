# Load the Rails application.
require_relative 'application'

# Initialize the Rails application.
Rails.application.initialize!

#load all the classes in the NexosisApi module
Dir["#{Rails.root}/app/models/NexosisApi/*.rb"].each { |file|
  require_dependency "NexosisApi/#{file[file.rindex('/') + 1...-3]}"
}