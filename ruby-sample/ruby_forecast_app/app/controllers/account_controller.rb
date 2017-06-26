require 'nexosis_api'
require 'csv'

class AccountController < ApplicationController
	def index
		params.permit(:uploaded)
		if(params["uploaded"])
			@uploadMessage = "Your dataset has been uploaded."
		end

		@account_balance = @api_client.get_account_balance
		@datasets = @api_client.list_datasets
		render
	end

	def sessions
		@sessions = @api_client.list_sessions
		render
	end

	def session_status
		params.require(:session_id)
		@session = @api_client.get_session(params["session_id"])
		render "session"
	end

	def upload
		uploaded_io = params["datafile"]
		dataset_name = params["dataset_name"]
		File.open(Rails.root.join('public', 'uploads', uploaded_io.original_filename), 'wb') do |file|
			file.write(uploaded_io.read)
		end
		csv = CSV.open(Rails.root.join('public', 'uploads', uploaded_io.original_filename), 'rb', headers: true)
		begin
			@api_client.create_dataset_csv(dataset_name, csv)
		rescue NexosisApi::HttpException => http_error
			@error_message = http_error.message
			render
			return
		end
		redirect_to action: 'index', :uploaded => true
	end

	def forecast
		params.require(:dataset_name)
		@dataset_name = params["dataset_name"]
	end

	def create_forecast
		params.require(:dataset_name)
		params.require(:target_column)
		params.require(:start_date)
		params.require(:end_date)
		params.permit(:is_estimate)
		estimate_only = params["is_estimate"] == "on" ? true : false
		begin
			if(estimate_only)		
				@forecast_result = @api_client.estimate_forecast_session params["dataset_name"], params["target_column"], params["start_date"], params["end_date"]
			else	
				@forecast_result = @api_client.create_forecast_session params["dataset_name"], params["target_column"], params["start_date"], params["end_date"]
			end
		rescue NexosisApi::HttpException => http_error
			@error = http_error
		end
		render "forecast"
	end

	def impact
		params.require(:dataset_name)
		@dataset_name = params["dataset_name"]
	end
	
	def create_impact
		params.require(:dataset_name)
		params.require(:target_column)
		params.require(:start_date)
		params.require(:end_date)
		params.require(:event_name)
		params.permit(:is_estimate)
		estimate_only = params["is_estimate"] == "on" ? true : false
		@event_name = params["event_name"]
		begin
			if(estimate_only)
				@impact_result = @api_client.estimate_impact_session(params["dataset_name"], params["target_column"], params["start_date"], params["end_date"] , params["event_name"])
			else
				@impact_result = @api_client.create_impact_session(params["dataset_name"], params["target_column"], params["start_date"], params["end_date"] , params["event_name"])
			end
		rescue NexosisApi::HttpException => http_error
			@error = http_error
		end
		render "impact"
	end
	
end

