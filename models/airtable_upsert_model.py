from pydantic import BaseModel, Field, EmailStr
from typing import Optional, Literal

class Conversations(BaseModel):
    """Type class for Conversations table"""
    conversation_id: str = Field(..., description="Unique conversation_id.")
    opportunity_id: str = Field(..., description="Opportunity id received from the inbound payload.")
    subscription_id: str = Field(..., description="Subscription id received from the inbound payload.")
    rooftop_name: str = Field(..., description="Rooftop name")
    lead_source: str = Field(..., description="Source / Provider from where the lead has been received [recordId].")
    customer_full_name: str = Field(..., description="Full name of the customer received from the inbound payload.")
    customer_email: Optional[EmailStr] = Field(description="Email of the customer extracted from the inbound payload.")
    customer_phone: str = Field(..., description="Customer phone number extracted from the inbound payload.")
    salesperson_assigned: str = Field(..., description="Salesperson which is assigned to handle the customer.")
    last_channel: Literal['sms', 'email'] = Field(..., description="The last channel which was used to communicate with the customer.")
    last_activity_at: str = Field(..., description="Last activity timestamp datetime -> string (strftime).")
    last_customer_message: str = Field (..., description="Last message received by the customer.")
    status: Literal['open', 'closed', 'suppressed', 'needs_review'] = Field(..., description="Current status of the lead")
    opted_out: bool = Field(...)
    linked_lead_record: str = Field(..., description="Requires a record id which will be used to link this cell with an existing lead record")
    needs_human_review: Optional[bool] = Field(description="When human intervention is required or not.")
    needs_human_review_reason: Optional[str] = Field(description="Reason for the human review.")
    ai_last_reply_at: Optional[str] = Field(description="Last AI reply timestamp datetime -> string (strftime).")
    customer_last_reply_at: Optional[str] = Field(description="Last reply timestamp by the customer datetime -> string (strftime).")
    message_count_total: Optional[int]
    message_count_inbound: Optional[int]
    message_count_outbound: Optional[int]


class Messages(BaseModel):
    """Type class for Messages table"""
    message_id: str = Field(..., description="Unique id for message received or sent")
    conversastion: str = Field(..., description="Reference to the conversation to which this message corresponds (recordId).")
    direction: Literal['inbound', 'outbound'] = Field(..., description="Direction of the message.")
    channel: Literal['sms', 'email'] = Field(..., description="Channel through which the message came in / was sent.")
    timestamp: str = Field(..., description="Timestamp of the message sent/received.")
    from_: str = Field(...)
    to: str = Field(...)
    subject: Optional[str] = Field(description="Only in case when the channel is email.")
    body_text: str = Field(..., description="Text body of the message sent or received (clean html tags in case of email).")
    body_html: Optional[str] = Field(..., description="Email body html (only valid for the case of email).")
    provider: str = Field(..., description="Providers of the lead.")
    opp_id: str = Field(..., description="Opportunity id received from the inbound payload, indexed for debugging.")
    delivery_status: Literal['sent', 'received', 'failed', 'unknown'] = Field(...)
